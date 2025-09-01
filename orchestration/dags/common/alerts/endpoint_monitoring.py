from common.config import ENV_SHORT_NAME


def create_recommendation_endpoint_monitoring_slack_block(
    endpoint_name: str,
    metabase_url: str,
    env_short_name: str = ENV_SHORT_NAME,
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":martial_arts_uniform:  Endpoint monitoring on {endpoint_name} is complete :martial_arts_uniform: ",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Voir le rapport :chart_with_upwards_trend:",
                        "emoji": True,
                    },
                    "url": metabase_url,
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Environnement: {env_short_name}"}
            ],
        },
    ]
