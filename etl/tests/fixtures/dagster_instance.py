"""
Dagster instance fixtures for testing.
"""

import os
import shutil

import pytest
from dagster._core.instance import DagsterInstance


@pytest.fixture(scope="session")
def dagster_instance(tmp_path_factory):
    """
    Create a Dagster instance for testing.
    Uses a temporary directory for storage.
    """
    # Create temporary directory for Dagster home
    dagster_home = tmp_path_factory.mktemp("dagster_test_home")

    # Set environment variable for Dagster home
    os.environ["DAGSTER_HOME"] = str(dagster_home)

    # Create storage directory
    storage_dir = dagster_home / "storage"
    storage_dir.mkdir(exist_ok=True)

    # Create a minimal Dagster instance for testing
    instance = DagsterInstance.ephemeral()

    yield instance

    # Cleanup
    if dagster_home.exists():
        shutil.rmtree(dagster_home, ignore_errors=True)


@pytest.fixture
def test_dagster_home(tmp_path):
    """Temporary Dagster home directory for a single test."""
    dagster_home = tmp_path / "dagster_home"
    dagster_home.mkdir()
    (dagster_home / "storage").mkdir()

    original_home = os.environ.get("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = str(dagster_home)

    yield dagster_home

    if original_home:
        os.environ["DAGSTER_HOME"] = original_home
    elif "DAGSTER_HOME" in os.environ:
        del os.environ["DAGSTER_HOME"]
