FORMAT_DICT = {
    "start_date": str,
    "end_date": str,
    "status": int,
    "response_id": str,
    "user_id": str,
    "distribution_channel": str,
    "question": str,
    "answer": str,
    "question_str": str,
    "question_id": str,
    "extra_data": str,
    "survey_id": str,
}

OPT_OUT_EXPORT_COLUMNS = {
    "contactId": "contact_id",
    "firstName": "first_name",
    "lastName": "last_name",
    "email": "email",
    "phone": "phone",
    "language": "language",
    "extRef": "ext_ref",
    "directoryUnsubscribed": "directory_unsubscribed",
    "directoryUnsubscribeDate": "directory_unsubscribe_date",
}

ANSWERS_SCHEMA = {
    "start_date": "STRING",
    "end_date": "STRING",
    "status": "INTEGER",
    "response_id": "STRING",
    "user_id": "STRING",
    "distribution_channel": "STRING",
    "question": "STRING",
    "answer": "STRING",
    "question_str": "STRING",
    "question_id": "STRING",
    "extra_data": "STRING",
    "survey_id": "STRING",
    "survey_int_id": "INTEGER",
}
