from common.config import ENV_SHORT_NAME, MLFLOW_URL


def create_algo_training_slack_block(
    models: str,
    mlflow_url: str = MLFLOW_URL,
    env_short_name: str = ENV_SHORT_NAME,
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":robot_face: Entraînement de l'algo {models} terminé ! :rocket:",
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


def create_item_embedding_slack_block(
    summary: str,
    env_short_name: str = ENV_SHORT_NAME,
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":robot_face: Embedding des items terminé ! :rocket:\n{summary}",
            },
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Environnement: {env_short_name}"}
            ],
        },
    ]


def create_finance_pricing_forecast_slack_block(
    models: str,
    mlflow_url: str = MLFLOW_URL,
    env_short_name: str = ENV_SHORT_NAME,
):
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":moneybag: Prévisions financières {models} terminées !",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Voir sur MLflow (Métriques & Prévisions)",
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


def create_finance_report_slack_block(
    summary: str,
    mlflow_url: str = MLFLOW_URL,
    env_short_name: str = ENV_SHORT_NAME,
):
    """Slack block for the finance "compte rendu" delivered after a forecast run.

    Args:
        summary: One-line French summary produced by the reporting job (usually an
            XCom reference rendered at runtime).
        mlflow_url: Link to the MLflow UI where the report artifacts live.
        env_short_name: Environment short name (dev/stg/prod).
    """
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":bar_chart: *Compte rendu des prévisions de pricing (DAF)*\n{summary}",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Voir le rapport sur MLflow (onglet report/)",
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
