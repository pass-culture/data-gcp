import os

from set_env import set_single_env_var


def test_set_single_env_var():
    set_single_env_var("VAR_1=VAL_1")
    assert os.environ["VAR_1"] == "VAL_1"
