import logging
import os

LOGGING_LEVEL_DEFAULT = "INFO"
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", LOGGING_LEVEL_DEFAULT)


def run_logging() -> None:
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    print(f"Logging Level is {LOGGING_LEVEL}")
    logging.debug("This is a debug message.")
    logging.info("This is an info message.")
    logging.warning("This is a warning message.")
    logging.error("This is an error message.")
    logging.critical("This is a critical message.")


if __name__ == "__main__":
    run_logging()
