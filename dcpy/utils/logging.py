import logging
from datetime import datetime
from pathlib import Path


class LoggingHandler(logging.StreamHandler):
    def __init__(self):
        super(LoggingHandler, self).__init__()

    def emit(self, record):
        # Ensure multi-line logs have consistent log formatting
        messages = record.msg.split("\n")
        for message in messages:
            record.msg = message
            super(LoggingHandler, self).emit(record)


class LoggingFileHandler(logging.FileHandler):
    """File handler with multi-line log support."""

    def emit(self, record):
        # Ensure multi-line logs have consistent log formatting
        messages = record.msg.split("\n")
        for message in messages:
            record.msg = message
            super(LoggingFileHandler, self).emit(record)


logging.root.setLevel(logging.INFO)

# Console handler
handler = LoggingHandler()
formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s:%(name)s:%(message)s", datefmt="%H:%M:%S"
)
handler.setFormatter(formatter)


class DcpyLogger(logging.Logger):
    """Custom logger with file output configuration support."""

    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self._file_handler = None

    def set_file_output(self, output_dir: str | Path):
        """Configure file logging to write to output_dir/diagnostics/logs.

        Args:
            output_dir: Build output directory path

        This method can be called to enable file logging after the logger is created.
        Safe to call multiple times - will only add the handler once.
        """
        if self._file_handler:
            return  # Already configured

        log_dir = Path(output_dir) / "diagnostics" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"build_{timestamp}.log"

        self._file_handler = LoggingFileHandler(str(log_file))
        # Use more detailed formatter for file logs
        file_formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s:%(name)s:%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._file_handler.setFormatter(file_formatter)
        self.addHandler(self._file_handler)

        self.info(f"Logging to file: {log_file}")


# Set custom logger class and create logger
logging.setLoggerClass(DcpyLogger)
logger: DcpyLogger = logging.getLogger("dcpy")  # type: ignore[assignment]
logger.addHandler(handler)
