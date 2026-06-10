from enum import StrEnum


class ActionType(StrEnum):
    ADD = "add"


class CommentType(StrEnum):
    NEW_EVENT = "new_event"


class ClusterRepresentantMethod(StrEnum):
    MAX_SIMILARITY = "max_similarity"
    MAX_EXACT_SIMILARITY = "max_exact_similarity"
