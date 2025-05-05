from common.config import ENV_SHORT_NAME, MLFLOW_URL


def create_algo_training_slack_block(
    experiment_name: str,
    mlflow_url: str = MLFLOW_URL,
    env_short_name: str = ENV_SHORT_NAME,
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":robot_face: Entraînement de l'algo {experiment_name} terminé ! :rocket:",
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


def render_block(**kwargs):
    experiment_name = kwargs["params"]["model_name"]
    block = create_algo_training_slack_block(experiment_name)
    return block
