"""Shared pytest fixtures."""

import shutil

import pytest

from jobs.common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    """One Spark session for the whole test run: starting the JVM takes seconds."""
    if shutil.which("java") is None:
        pytest.skip("Java not found: run the Spark tests in Docker")
    session = get_spark("tests")
    yield session
    session.stop()
