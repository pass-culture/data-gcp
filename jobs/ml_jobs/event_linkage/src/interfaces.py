from enum import StrEnum


class ActionType(StrEnum):
    ADD = "add"
    REMOVE = "remove"


class CommentType(StrEnum):
    NEW_EVENT = "new_event"
    LINKED_TO_EXISTING_EVENT = "linked_to_existing_event"
    REMOVED_EVENT = "removed_event"
    FULL_RESET = "full_reset"


class ClusterRepresentantMethod(StrEnum):
    MAX_SIMILARITY = "max_similarity"
    MAX_EXACT_SIMILARITY = "max_exact_similarity"
