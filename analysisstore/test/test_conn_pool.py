from ..client.commands import AnalysisClient
from .testing import TESTING_CONFIG
import pytest


def test_client_badconf():
    config = {"host": "localhost"}
    pytest.raises(KeyError, AnalysisClient, config)
    config["port"] = TESTING_CONFIG["port"]
    conn = AnalysisClient(config)
    conn.host == TESTING_CONFIG["host"]
    conn.port == TESTING_CONFIG["port"]
