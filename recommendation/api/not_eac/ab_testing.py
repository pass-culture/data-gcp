


def query_ab_testing_table(
        user_id,
):
    start = time.time()

    with create_db_connection() as connection:
        request_response = connection.execute(
            text(f"SELECT groupid FROM {AB_TESTING_TABLE} WHERE userid= :user_id"),
            user_id=str(user_id),
        ).scalar()

    log_duration(f"query_ab_testing_table for {user_id}", start)
    return request_response