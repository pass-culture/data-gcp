import logging
import os

logger = logging.getLogger()


PATH_TO_ENV_DIR = os.path.dirname(os.path.realpath(__file__))


def set_single_env_var(string_definition):
    var_name, var_value = string_definition.split("=")
    os.environ[var_name] = var_value.strip()
    logger.info(f"Set {var_name}")


def set_env_vars():
    if os.path.isfile(os.path.join(PATH_TO_ENV_DIR, ".env.local")):
        path_to_env_file = os.path.join(PATH_TO_ENV_DIR, ".env.local")
    else:
        return
    with open(path_to_env_file) as env_file:
        for var in env_file.readlines():
            if not var.startswith("#"):
                set_single_env_var(var)
