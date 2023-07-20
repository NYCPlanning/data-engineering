import subprocess


def git_username():
    return (
        subprocess.run(["git", "config", "user.name"], stdout=subprocess.PIPE)
        .stdout.strip()
        .decode()
    )


def git_branch():
    return (
        subprocess.run(
            ["git", "rev-parse", "--symbolic-full-name", "--abbrev-ref", "HEAD"],
            stdout=subprocess.PIPE,
        )
        .stdout.strip()
        .decode()
    )
