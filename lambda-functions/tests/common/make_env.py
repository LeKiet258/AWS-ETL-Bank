# flake8: noqa: E501
import os

_env_vars = {
    "STAGE": lambda: os.environ["STAGE"]
}

for env_var, resolve in _env_vars.items():
    assert callable(resolve)


def fake_env_vars():
    return {e: "not_defined" for e in _env_vars.keys()}


def resolve_env_vars():
    return {e: r() for e, r in _env_vars.items()}


def get_env_list():
    return list(_env_vars.keys())
