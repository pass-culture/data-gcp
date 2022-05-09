import random
from sqlalchemy import text
from utils import AB_TESTING_TABLE
from refacto_api.tools.db_connection import create_db_connection


def query_ab_testing_table(
    user_id,
):

    with create_db_connection() as connection:
        request_response = connection.execute(
            text(f"SELECT groupid FROM {AB_TESTING_TABLE} WHERE userid= :user_id"),
            user_id=str(user_id),
        ).scalar()

    return request_response


def ab_testing_assign_user(user_id):
    groups = ["A", "B", "C"]
    group_id = random.choice(groups)

    with create_db_connection() as connection:
        connection.execute(
            text(
                f"INSERT INTO {AB_TESTING_TABLE}(userid, groupid) VALUES (:user_id, :group_id)"
            ),
            user_id=user_id,
            group_id=str(group_id),
        )

    return group_id
