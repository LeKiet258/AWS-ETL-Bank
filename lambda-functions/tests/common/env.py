import os
from collections import namedtuple

from .make_env import get_env_list

EnvVars = namedtuple("EnvVars", [e.lower() for e in get_env_list()])


def _get_required_env(var_name: str) -> str:
    env_var = os.getenv(var_name)

    assert env_var is not None, f"environment variable {var_name} is required"

    return env_var


def get_env_vars():
    kwargs = {e.lower(): _get_required_env(e) for e in get_env_list()}

    return EnvVars(**kwargs)
