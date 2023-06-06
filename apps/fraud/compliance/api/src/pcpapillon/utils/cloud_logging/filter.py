import logging
import re
from google.cloud.logging_v2.handlers import CloudLoggingFilter
from pcpapillon.utils.env_vars import (
    http_request_context,
    cloud_trace_context,
)


class GoogleCloudLogFilter(CloudLoggingFilter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.http_request = http_request_context.get()

        trace = cloud_trace_context.get()
        split_header = trace.split("/", 1)

        record.trace = f"projects/{self.project}/traces/{split_header[0]}"

        # header_suffix = split_header[1]
        # record.span_id = re.findall(r'^\w+', header_suffix)[0]

        super().filter(record)

        return True
