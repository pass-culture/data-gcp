QPI_ANSWERS_SCHEMA = [
    {"name": "culturalsurvey_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "landed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "submitted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "form_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "platform", "type": "STRING", "mode": "NULLABLE"},
    {
        "name": "answers",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "choice", "type": "STRING", "mode": "NULLABLE"},
            {"name": "choices", "type": "STRING", "mode": "REPEATED"},
            {"name": "other", "type": "STRING", "mode": "NULLABLE"},
            {"name": "question_id", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
]
