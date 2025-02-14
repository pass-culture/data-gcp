from git import Repo
from loguru import logger


def get_git_current_branch() -> str:
    GIT_UNKNOWN_BRANCH = "git_unknown_branch"
    try:
        repo = Repo(".", search_parent_directories=True)
        branch_name = repo.active_branch.name
        return branch_name
    except Exception as e:
        logger.error("Cannot retrieve git branch name : %s", e)
        return GIT_UNKNOWN_BRANCH
