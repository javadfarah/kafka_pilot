import json
import logging
import sys


# Create a custom formatter for JSON logs
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'message': record.getMessage(),
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'module': record.module,
            'func_name': record.funcName,
            'line_no': record.lineno,
        }
        # Check if there's exception information in the log record
        if record.exc_info:
            exc_type, exc_value, exc_traceback = record.exc_info
            log_record['exception'] = {
                'type': str(exc_type),
                'message': str(exc_value),
                'traceback': repr(exc_traceback)
            }
        return json.dumps(log_record)


# Create a logger
logger = logging.getLogger('json_logger')
logger.setLevel(logging.DEBUG)

json_formatter = JSONFormatter()
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(json_formatter)
logger.addHandler(stream_handler)

if __name__ == '__main__':
    logger.debug("************")
    logger.info("This is an informational message")
    logger.debug("212121")
    logger.debug({"key": "value"})
