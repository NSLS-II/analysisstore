from ..commands import AnalysisClient
from .testing import TESTING_CONFIG
import pytest


def setup():
    pass


def test_client_default():
    pytest.raises(TypeError, AnalysisClient)


def test_client_wconf():
    config = {'host': TESTING_CONFIG['host'],
              'port': TESTING_CONFIG['port']}
    conn = AnalysisClient(config)


def test_client_badconf():
    config = {'host': 'localhost'}
    pytest.raises(KeyError, AnalysisClient, config)
