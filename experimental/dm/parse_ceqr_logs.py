from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import gzip
import re
import typer
from rich.progress import track
from typing import TypedDict

from dcpy.utils.s3 import list_objects, download_file

app = typer.Typer(
    add_completion=False,
    help="Download, parse, and analyze CEQR data hub access logs from S3",
)

LOG_DIR = Path(__file__).parent / ".data" / "ceqr_logs"

# Constants
BUCKET = "gde-logging"
BUCKET_PREFIX = "ceqr-data-hub"
CSV_FILENAME = "ceqr_logs_parsed.csv"
BOT_KEYWORDS = [
    "bot",
    "crawler",
    "spider",
    "scraper",
    "curl",
    "wget",
    "python-requests",
]
HIGH_VOLUME_THRESHOLD = 0.05  # Flag IPs with more than 5% of traffic

# Compile regex once for performance
LOG_LINE_PATTERN = re.compile(
    r'(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" "([^"]*)"'
)


class LogEntry(TypedDict, total=False):
    """Type definition for a parsed log entry"""

    bucket_owner: str
    bucket: str
    timestamp: str
    remote_ip: str
    requester: str | None
    request_id: str
    operation: str
    key: str
    request_uri: str
    http_status: int | None
    error_code: str | None
    bytes_sent: int | None
    object_size: int | None
    total_time_ms: int | None
    turn_around_time_ms: int | None
    referer: str | None
    user_agent: str | None


def parse_date_from_filename(filename: str) -> datetime | None:
    """Extract date from log filename like spaces-origin-2025-10-22-19-55-00-ceqr-data-hub-9972d4e2.log.gz"""
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})", filename)
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        return datetime(year, month, day, hour, minute, second)
    return None


def parse_log_line(line: str) -> LogEntry | None:
    """Parse a single S3/Spaces access log line

    Format reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
    """
    if not line.strip():
        return None

    match = LOG_LINE_PATTERN.match(line)
    if not match:
        print(f"Failed to parse line: {line[:100]}...")
        return None

    try:
        groups = match.groups()
        return {
            "bucket_owner": groups[0],
            "bucket": groups[1],
            "timestamp": groups[2],  # 07/Jan/2026:18:31:03 +0000
            "remote_ip": groups[3],
            "requester": groups[4] if groups[4] != "-" else None,
            "request_id": groups[5],
            "operation": groups[6],  # s3:GetObject, s3:ListBucket, etc.
            "key": groups[7],  # /bucket/path/to/file
            "request_uri": groups[8],  # Full HTTP request
            "http_status": int(groups[9]) if groups[9] != "-" else None,
            "error_code": groups[10] if groups[10] != "-" else None,
            "bytes_sent": int(groups[11]) if groups[11] != "-" else None,
            "object_size": int(groups[12]) if groups[12] != "-" else None,
            "total_time_ms": int(groups[13]) if groups[13] != "-" else None,
            "turn_around_time_ms": int(groups[14]) if groups[14] != "-" else None,
            "referer": groups[15] if groups[15] != "-" else None,
            "user_agent": groups[16] if groups[16] != "-" else None,
        }
    except (IndexError, ValueError) as e:
        print(f"Failed to parse line: {line[:100]}... Error: {e}")
        return None


def download_ceqr_logs(days_back: int = 30, sync: bool = True) -> None:
    """Download CEQR log files from the last month"""
    bucket = BUCKET
    prefix = BUCKET_PREFIX

    # Create download folder next to this script
    download_dir = LOG_DIR
    download_dir.mkdir(parents=True, exist_ok=True)

    # Get cutoff date
    cutoff_date = datetime.now() - timedelta(days=days_back)

    print(f"Fetching log files from {bucket}/{prefix}")
    objects = list_objects(bucket, prefix)

    # Filter objects by date
    files_to_download = []
    for obj in objects:
        key = obj["Key"]
        file_date = parse_date_from_filename(key)
        if file_date and file_date >= cutoff_date:
            files_to_download.append(obj)

    print(f"Found {len(files_to_download)} log files from the last {days_back} days")

    # If sync mode, skip already downloaded files
    if sync:
        existing_files = {f.name for f in download_dir.glob("*.log.gz")}
        files_to_download = [
            obj
            for obj in files_to_download
            if Path(obj["Key"]).name not in existing_files
        ]
        print(f"After sync check: {len(files_to_download)} files to download")

    if not files_to_download:
        print("No new files to download")
        return

    # Download each file
    for obj in files_to_download:
        key = obj["Key"]
        filename = Path(key).name
        download_file(bucket, key, download_dir / filename)

    print(f"Downloaded {len(files_to_download)} files to {download_dir}")


def parse_all_logs(
    log_dir: Path | None = None, save_csv: bool = False
) -> list[LogEntry]:
    """Parse all downloaded log files and return as list of dicts

    Args:
        log_dir: Directory containing log files (defaults to LOG_DIR)
        save_csv: If True, save parsed logs to CSV file
    """
    if log_dir is None:
        log_dir = LOG_DIR

    all_logs: list[LogEntry] = []
    log_files = sorted(log_dir.glob("*.log.gz"))

    if not log_files:
        print(f"No log files found in {log_dir}")
        return all_logs

    print(f"Parsing {len(log_files)} log files...")
    for log_file in track(log_files, description="Parsing logs"):
        with gzip.open(log_file, "rt") as f:
            for line in f:
                parsed = parse_log_line(line)
                if parsed:
                    all_logs.append(parsed)

    print(f"Parsed {len(all_logs)} log entries")

    if save_csv and all_logs:
        output_file = log_dir / CSV_FILENAME
        df = pd.DataFrame(all_logs)
        df.to_csv(output_file, index=False)
        print(f"Saved to {output_file}")

    return all_logs


def load_logs_from_csv() -> list[LogEntry]:
    """Load parsed logs from CSV file"""
    csv_file = LOG_DIR / CSV_FILENAME
    if not csv_file.exists():
        typer.echo(f"Error: {csv_file} not found. Run 'parse' command first.", err=True)
        raise typer.Exit(1)
    print(f"Loading logs from {csv_file}")
    df = pd.read_csv(csv_file)
    return df.to_dict("records")  # type: ignore


def analyze_logs(logs: list[LogEntry]) -> None:
    """Analyze parsed logs and print detailed statistics"""
    if not logs:
        print("No logs to analyze")
        return

    print("\n--- Summary ---")
    print(f"Total requests: {len(logs)}")

    # Count by operation
    operations = Counter(log["operation"] for log in logs)
    print(f"Operations: {dict(operations)}")

    # Count by HTTP status
    statuses = Counter(log.get("http_status") for log in logs if log.get("http_status"))
    print(f"HTTP statuses: {dict(statuses)}")

    # Top files accessed
    top_files = Counter(log.get("key") for log in logs if log.get("key")).most_common(
        10
    )
    print(f"\nTop 10 most accessed files:")
    for file, count in top_files:
        print(f"  {count:4d} - {file}")

    # Investigate potential bot activity
    print("\n--- Bot Activity Analysis ---")

    # Check "-" key requests (likely probes)
    dash_requests = [log for log in logs if log.get("key") == "-"]
    print(f"\nRequests with no key ('-'): {len(dash_requests)}")
    if dash_requests:
        dash_ips = Counter(log["remote_ip"] for log in dash_requests)
        print(f"Top IPs making '-' requests:")
        for ip, count in dash_ips.most_common(5):
            print(f"  {count:4d} - {ip}")

        dash_ops = Counter(log["operation"] for log in dash_requests)
        print(f"Operations on '-' requests: {dash_ops}")

    # Check GetBucketOwnershipControls requests
    ownership_requests = [
        log for log in logs if log.get("operation") == "s3:GetBucketOwnershipControls"
    ]
    if ownership_requests:
        print(f"\ns3:GetBucketOwnershipControls requests: {len(ownership_requests)}")
        ownership_ips = Counter(log["remote_ip"] for log in ownership_requests)
        print(f"Top IPs making ownership control requests:")
        for ip, count in ownership_ips.most_common(5):
            print(f"  {count:4d} - {ip}")

        ownership_agents = Counter(
            log.get("user_agent") for log in ownership_requests if log.get("user_agent")
        )
        print(f"User agents: {ownership_agents.most_common(3)}")

    # Check user agent patterns
    print(f"\n--- User Agent Analysis ---")
    agent_counter = Counter(
        log.get("user_agent") for log in logs if log.get("user_agent")
    )
    print(f"Top 10 user agents:")
    for agent, count in agent_counter.most_common(10):
        # Truncate long user agents
        agent_display = agent[:80] + "..." if len(agent) > 80 else agent
        print(f"  {count:4d} - {agent_display}")

    # Identify likely bots by user agent
    bot_requests = [
        log
        for log in logs
        if log.get("user_agent")
        and any(keyword in log["user_agent"].lower() for keyword in BOT_KEYWORDS)
    ]
    print(
        f"\nLikely bot requests (by user agent): {len(bot_requests)} ({100 * len(bot_requests) / len(logs):.1f}%)"
    )

    # Check for suspicious IP patterns (single IP with many requests)
    print(f"\n--- IP Analysis ---")
    ip_counter = Counter(log["remote_ip"] for log in logs)
    print(f"Top 10 IPs by request count:")
    for ip, count in ip_counter.most_common(10):
        print(f"  {count:4d} - {ip} ({100 * count / len(logs):.1f}%)")

    # High-volume IPs (more than threshold)
    high_volume_ips = [
        ip
        for ip, count in ip_counter.items()
        if count > len(logs) * HIGH_VOLUME_THRESHOLD
    ]
    if high_volume_ips:
        print(
            f"\nHigh-volume IPs (>{HIGH_VOLUME_THRESHOLD * 100:.0f}% of traffic): {len(high_volume_ips)}"
        )


@app.command()
def download(
    days_back: int = typer.Option(
        30, "--days-back", help="Number of days back to download logs"
    ),
    no_sync: bool = typer.Option(
        False, "--no-sync", help="Disable sync mode (re-download all files)"
    ),
) -> None:
    """Download CEQR log files from S3"""
    download_ceqr_logs(days_back=days_back, sync=not no_sync)


@app.command()
def parse(
    save_csv: bool = typer.Option(
        True, "--save-csv/--no-save-csv", help="Save parsed logs to CSV"
    ),
) -> None:
    """Parse downloaded log files"""
    parse_all_logs(save_csv=save_csv)


@app.command()
def analyze(
    from_csv: bool = typer.Option(
        False, "--from-csv", help="Load logs from CSV instead of parsing"
    ),
) -> None:
    """Analyze parsed log files"""
    logs = load_logs_from_csv() if from_csv else parse_all_logs()
    analyze_logs(logs)


@app.command()
def all(
    days_back: int = typer.Option(
        30, "--days-back", "-d", help="Number of days back to download logs"
    ),
    no_sync: bool = typer.Option(
        False, "--no-sync", help="Disable sync mode (re-download all files)"
    ),
) -> None:
    """Download, parse, and analyze CEQR logs (all steps)"""
    print("=== Step 1: Download ===")
    download_ceqr_logs(days_back=days_back, sync=not no_sync)

    print("\n=== Step 2: Parse & Save ===")
    logs = parse_all_logs(save_csv=True)

    if not logs:
        print("No logs parsed, exiting.")
        return

    print("\n=== Step 3: Analyze ===")
    analyze_logs(logs)


if __name__ == "__main__":
    app()
