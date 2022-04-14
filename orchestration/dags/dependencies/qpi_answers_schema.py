QPI_ANSWERS_SCHEMA = [
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "submitted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {
        "name": "answers",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "question_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "choices", "type": "STRING", "mode": "REPEATED"},
        ],
    },
]

