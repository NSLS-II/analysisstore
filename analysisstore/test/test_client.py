from ..client.commands import AnalysisClient
from .testing import TESTING_CONFIG
import pytest
from .testing import astore_setup, astore_teardown
import requests
from doct import Document
import uuid


class TestConnClient:
    def setup_class(self):
        self.proc = astore_setup()
        self.conn = AnalysisClient(TESTING_CONFIG)

    def test_conn_switch(self):
        w_conf = dict(host='wrong_host',
                      port=0)
        tmp_conn = AnalysisClient(w_conf)
        tmp_conn.host == w_conf['host']
        tmp_conn.port == 0
        pytest.raises(requests.exceptions.ConnectionError,
                      tmp_conn.connection_status)

    def test_urls(self):
        """Catch potentially annoying and difficult to debug typos"""
        base_test_url = 'http://{}:{}/'.format(TESTING_CONFIG['host'],
                                               TESTING_CONFIG['port'])
        self.conn._host_url == base_test_url
        self.conn.aheader_url == base_test_url + 'analysis_header'
        self.conn.atail_url == base_test_url + 'analysis_tail'
        self.conn.dref_url == base_test_url + 'data_reference'
        self.conn.dref_header_url == base_test_url + 'data_reference_header'

    def test_doc_or_uid_to_uid(self):
        m_uid=str(uuid.uuid4())
        test_dict = {'name': 'test_doc', 'uid': m_uid}
        m_uid == self.conn._doc_or_uid_to_uid(test_dict)

    def test_post_fact(self):
       pld = {'data': 'bogus'}
       sig = 'bogus'
       res = self.conn._post_factory(signature=sig,
                                     payload=pld)
       res['payload'] == pld
       res['signature'] == sig

    def test_query_fact(self):
       pld = {'data': 'bogus'}
       sig = 'bogus'
       res = self.conn._query_factory(signature=sig,
                                     payload=pld)
       res['payload'] == pld
       res['signature'] == sig

    def test_header_insert(self):
       pytest.raises(TypeError, self.conn.insert_analysis_header)

    def test_tail_insert(self):
        pass

    def test_dref_header_insert(self):
        pass

    def test_dref_insert(self):
        pass

    def test_header_find(self):
        pass

    def test_tail_find(self):
        pass

    def test_dref_header_find(self):
        pass

    def test_dref_find(self):
        pass


    def teardown_class(self):
        astore_teardown(self.proc)
