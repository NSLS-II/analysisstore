from ..client.commands import AnalysisClient
from .conftest import testing_config
import pytest


def test_client_badconf():
    config = {"host": "localhost"}
    pytest.raises(KeyError, AnalysisClient, config)
    config["service_port"] = testing_config["service_port"]
    conn = AnalysisClient(config)
    conn.host == testing_config["host"]
    conn.service_port == testing_config["service_port"]
