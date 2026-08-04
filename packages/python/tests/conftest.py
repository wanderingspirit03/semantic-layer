from __future__ import annotations

import pytest

from semantic_layer import reset_capture_for_tests


@pytest.fixture(autouse=True)
def reset_capture_singleton() -> None:
    reset_capture_for_tests()
    yield
    reset_capture_for_tests()
