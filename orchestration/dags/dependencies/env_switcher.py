from dependencies.config import ENV_SHORT_NAME


def env_switcher():
    next_steps = ["dummy_task"]

    if ENV_SHORT_NAME == "prod":
        next_steps.append("copy_firebase_data")

    return next_steps
