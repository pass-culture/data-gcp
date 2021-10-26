import time
from sqlalchemy import text

from utils import create_db_connection, log_duration


MACRO_CATEGORIES_TYPE_MAPPING = {
    "CINEMA": ["CINEMA"],
    "FILM": ["FILM"],
    "JEU": ["JEU"],
    "LIVRE": ["LIVRE"],
    "MUSEE": ["MUSEE"],
    "MUSIQUE": ["MUSIQUE_LIVE", "MUSIQUE_ENREGISTREE"],
    "PRATIQUE_ART": ["PRATIQUE_ART"],
    "SPECTACLE": ["SPECTACLE"],
    "INSTRUMENT": ["INSTRUMENT"],
    "MEDIA": ["MEDIA"],
    "AUTRE": ["CONFERENCE_RENCONTRE", "BEAUX_ARTS"],
}

QPI_TO_CAT = {
    "Q0": "FILM",
    "Q1": "MUSIQUE",
    "Q2": "MEDIA",
    "Q3": "LIVRE",
    "Q4": "MEDIA",
    "Q5": "INSTRUMENT",
    "Q6": "JEU",
    "Q7": "PRATIQUE_ART",
    "Q8": "",
    "Q9": "CINEMA",
    "Q10": "LIVRE",
    "Q11": "SPECTACLE",
    "Q12": "MUSEE",
    "Q13": "SPECTACLE",
    "Q14": "SPECTACLE",
    "Q15": "JEU",
    "Q16": "AUTRE",
    "Q17": "PRATIQUE_ART",
    "Q18": "",
    "Q19": "SPECTACLE",
    "Q20": "SPECTACLE",
    "Q21": "SPECTACLE",
    "Q22": "SPECTACLE",
    "Q23": "SPECTACLE",
    "Q24": "SPECTACLE",
    "Q25": "SPECTACLE",
    "Q26": "CINEMA",
    "Q27": "LIVRE",
    "Q28": "MUSIQUE",
    "Q29": "SPECTACLE",
    "Q30": "CINEMA",
}


def get_cold_start_status(user_id: int) -> bool:
    start = time.time()
    cold_start_query = text(
        """
        SELECT bookings_count
        FROM number_of_bookings_per_user
        WHERE user_id= :user_id;
        """
    )

    with create_db_connection() as connection:
        query_result = connection.execute(
            cold_start_query, user_id=str(user_id)
        ).fetchone()

    bookings_count = query_result[0] if query_result is not None else 0
    user_cold_start_status = bookings_count < 2
    log_duration(f"get_cold_start_status", start)
    return user_cold_start_status


def get_cold_start_categories(user_id: int) -> list:
    start = time.time()
    NbOfQPIquestions = 31
    qpi_questions = [f"Q{i}" for i in range(NbOfQPIquestions)]
    cold_start_query = text(
        f"SELECT {', '.join(qpi_questions)} FROM qpi_answers WHERE user_id = :user_id;"
    )

    with create_db_connection() as connection:
        query_result = connection.execute(
            cold_start_query,
            user_id=str(user_id),
        ).fetchall()

    cold_start_categories = []
    if len(query_result) == 0:
        return []
    for question_index, answers in enumerate(query_result[0]):
        if (
            answers
            and len(
                MACRO_CATEGORIES_TYPE_MAPPING[QPI_TO_CAT[qpi_questions[question_index]]]
            )
            > 0
        ):
            cold_start_categories.extend(
                MACRO_CATEGORIES_TYPE_MAPPING[QPI_TO_CAT[qpi_questions[question_index]]]
            )
    log_duration("get_cold_start_categories", start)
    return list(set(cold_start_categories))
