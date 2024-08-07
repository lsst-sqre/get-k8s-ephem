"""Ensure object imports."""

import get_k8s_ephem


def test_import() -> None:
    executor = get_k8s_ephem.executor.Executor()
    assert executor is not None
