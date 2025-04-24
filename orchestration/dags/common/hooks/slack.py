import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import requests

HTTP_HOOK = "https://hooks.slack.com/services/"
DEFAULT_HEADERS = {"Content-Type": "application/json"}


@dataclass
class SlackGroup:
    slack_id: str
    name: str


class SlackTeam(Enum):
    DS = SlackGroup(slack_id="S08CVKQ4K9S", name="Data Science")
    DE = SlackGroup(slack_id="S08CT44F7J6", name="Data Engineering")

    @property
    def name(self) -> str:
        return self.value.name

    @property
    def slack_id(self) -> str:
        return self.value.slack_id

    def get_slack_message(self, ping: bool) -> str:
        if ping:
            return f"<!subteam^{self.slack_id}>"
        else:
            return f"{self.name}"


class SlackHook:
    def __init__(self, webhook_token):
        self.webhook_token = webhook_token

    def send_message(
        self, message: Optional[str] = None, block: Optional[List[dict]] = None
    ):
        if message:
            response = requests.post(
                f"{HTTP_HOOK}{self.webhook_token}",
                data=json.dumps({"text": f"{message}"}),
                headers=DEFAULT_HEADERS,
            )
        elif block:
            response = requests.post(
                f"{HTTP_HOOK}{self.webhook_token}",
                data=json.dumps({"blocks": block}),
                headers=DEFAULT_HEADERS,
            )
        else:
            raise ValueError("No message or blocks provided")

        if response.status_code != 200:
            raise ValueError(
                f"Request to Slack returned an error {response.status_code}, response: {response.text}"
            )
