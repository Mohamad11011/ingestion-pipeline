from __future__ import annotations

import importlib.util
import itertools
import os
from pathlib import Path
from types import ModuleType

from config.settings import get_settings

DEFINITIONS = Path(__file__).resolve().parents[1] / "orchestration" / "dagster" / "definitions.py"
_counter = itertools.count()


def load_definitions(partition_size: str = "monthly") -> ModuleType:
    """Import a fresh copy of the Dagster definitions; it reads settings at import time."""
    previous = os.environ.get("PARTITION_SIZE")
    os.environ["PARTITION_SIZE"] = partition_size
    get_settings.cache_clear()
    try:
        name = f"_wr_definitions_{next(_counter)}"
        spec = importlib.util.spec_from_file_location(name, DEFINITIONS)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            os.environ.pop("PARTITION_SIZE", None)
        else:
            os.environ["PARTITION_SIZE"] = previous
        get_settings.cache_clear()
