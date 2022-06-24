import random
from sqlalchemy import text
from pcreco.utils.env_vars import AB_TESTING_TABLE, AB_TESTING_GROUPS
from pcreco.utils.db.db_connection import create_db_connection
from typing import Any


def query_ab_testing_table(
    user_id,
) -> Any:

    connection = create_db_connection()
    request_response = connection.execute(
        text(f"SELECT groupid FROM {AB_TESTING_TABLE} WHERE userid= :user_id"),
        user_id=str(user_id),
    ).scalar()

    return request_response


def ab_testing_assign_user(user_id) -> str:
    groups = AB_TESTING_GROUPS
    group_id = random.choice(groups)

    connection = create_db_connection()
    connection.execute(
        text(
            f"INSERT INTO {AB_TESTING_TABLE}(userid, groupid) VALUES (:user_id, :group_id)"
        ),
        user_id=user_id,
        group_id=str(group_id),
    )
    return group_id
