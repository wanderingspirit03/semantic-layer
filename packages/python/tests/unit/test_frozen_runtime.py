from __future__ import annotations

import os
import platform


def test_exact_runner_uses_frozen_python_runtime() -> None:
    expected = os.environ.get("SEMANTIC_LAYER_EXPECTED_PYTHON_VERSION")
    if expected is not None:
        assert platform.python_version() == expected
