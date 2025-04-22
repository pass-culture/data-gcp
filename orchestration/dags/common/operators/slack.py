from typing import List, Optional

from common.hooks.slack import SlackHook

from airflow.operators.base_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class SendSlackMessageOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        webhook_token,
        message: Optional[str] = None,
        block: Optional[List[dict]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.webhook_token = webhook_token
        self.message = message
        self.block = block

    def execute(self, context):
        hook = SlackHook(self.webhook_token)
        hook.send_message(self.message, self.block)
