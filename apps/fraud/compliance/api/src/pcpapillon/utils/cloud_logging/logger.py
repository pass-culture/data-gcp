from fastapi.logger import logger


class CustomLogger:
    def info(
        self,
        message=None,
        extra=None,
    ):
        log_entry = {
            "message": message,
            "extra": extra,
        }
        logger.info(log_entry)
        return
