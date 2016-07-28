from ..client.commands import AnalysisClient
from .testing import TESTING_CONFIG
import pytest
from .testing import astore_setup, astore_teardown



class TestConnPool:
    def setup_class(self):
        self.proc = astore_setup()

    def test_client_default(self):
        pytest.raises(TypeError, AnalysisClient)


    def test_client_wconf(self):
        config = {'host': TESTING_CONFIG['host'],
                'port': TESTING_CONFIG['port']}
        conn = AnalysisClient(config)

    def test_client_badconf(self):
        config = {'host': 'localhost'}
        pytest.raises(KeyError, AnalysisClient, config)
        config['port'] = TESTING_CONFIG['port']
        conn = AnalysisClient(config)
        conn.host == TESTING_CONFIG['host']
        conn.port == TESTING_CONFIG['port']

    def teardown_class(self):
        astore_teardown(self.proc)
