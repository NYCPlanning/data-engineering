import logging


class LoggingHandler(logging.StreamHandler):
    def __init__(self):
        super(LoggingHandler, self).__init__()

    def emit(self, record):
        # Ensure multi-line logs have consistent log formatting
        messages = record.msg.split("\n")
        for message in messages:
            record.msg = message
            super(LoggingHandler, self).emit(record)


logging.root.setLevel(logging.INFO)

handler = LoggingHandler()
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)

logger = logging.getLogger("dcpy")
logger.addHandler(handler)
