from common.config import EXTERNAL_REPORTING_DRIVE_URL


def create_reporting_slack_block(
    external_reporting_drive_url: str = EXTERNAL_REPORTING_DRIVE_URL,
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":robot_face: Les reportings DRAC sont prêts ! ",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Voir les reportings :chart_with_upwards_trend:",
                    },
                    "url": external_reporting_drive_url,
                }
            ],
        },
    ]
