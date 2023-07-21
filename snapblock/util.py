"""Provides common utility functions"""

import re
import os
import sys
import logging

# Set a standard logger using STDOUT
# When importing this module, you can set LOGGER to a different logger if desired
SB_LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
SB_LOGGER.addHandler(handler)
SB_LOGGER.setLevel(logging.INFO)


def sizeof_fmt(num: int) -> str:
    """Takes a number of bytes and returns a human-readable representation"""

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} PB"


def reveal_secrets(json_obj) -> dict:
    """Looks for strings within a JSON-like object that start with '<secret>' and replaces these
    with secrets. Checks different secret-store options until it finds the secret.
    For example, the value '<secret>my-secret' would be replaced with the my-secret value in
    whichever secret store it is available."""

    def recursive_reveal(obj):
        if isinstance(obj, dict):
            return {k: recursive_reveal(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [recursive_reveal(i) for i in obj]
        if isinstance(obj, str) and obj.startswith("<secret>"):
            return _get_secret(obj[8:])
        return obj

    return recursive_reveal(json_obj)


def _get_secret(secret_name):
    # pylint:disable=import-outside-toplevel
    # Add more secret store options in here over time!
    if "prefect" in sys.modules:
        try:
            from prefect.blocks.system import Secret

            secret = Secret.load(secret_name).get()
            SB_LOGGER.info("Extracted value for %s from Prefect Secret", secret_name)
            return secret
        except (ValueError, ImportError, ModuleNotFoundError):
            pass
    # Last resort, check environmental variables
    # Convert to env var format
    var_name = re.sub(r"\W+", "_", secret_name).upper()
    try:
        return os.environ[var_name]
    except KeyError as err:
        raise ValueError(
            f"Secret {secret_name} could not be found in any secret store, "
            f"and {var_name} not found in environmental variable"
        ) from err
