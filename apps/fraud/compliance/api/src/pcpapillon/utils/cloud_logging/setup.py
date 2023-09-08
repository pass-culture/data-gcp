import logging

import google.cloud.logging
from fastapi.logger import logger
from pcpapillon.utils.cloud_logging.filter import GoogleCloudLogFilter
from pcpapillon.utils.cloud_logging.logger import CustomLogger
from pcpapillon.utils.env_vars import isAPI_LOCAL


def setup_logging():
    if isAPI_LOCAL:
        api_logger = logging.getLogger("uvicorn")
    else:
        client = google.cloud.logging.Client()
        handler = client.get_default_handler()
        handler.setLevel(logging.DEBUG)
        handler.filters = []
        handler.addFilter(GoogleCloudLogFilter(project=client.project))
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        api_logger = CustomLogger()
    return api_logger
