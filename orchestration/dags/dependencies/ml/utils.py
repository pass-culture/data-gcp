from common.config import MLFLOW_URL, ENV_SHORT_NAME


def create_algo_training_slack_block(
    mlflow_url: str = MLFLOW_URL, env_short_name: str = ENV_SHORT_NAME
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":robot_face: Entraînement de l'algo terminé ! :rocket:",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Voir les métriques :chart_with_upwards_trend:",
                        "emoji": True,
                    },
                    "url": mlflow_url,
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
