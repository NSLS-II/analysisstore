from ..client.commands import AnalysisClient
from .testing import TESTING_CONFIG
import pytest
import time
import requests
import uuid


def test_conn_switch(astore_client):
    w_conf = dict(host="wrong_host", port=0)
    tmp_conn = AnalysisClient(w_conf)
    tmp_conn.host == w_conf["host"]
    tmp_conn.port == 0
    pytest.raises(requests.exceptions.ConnectionError, tmp_conn.connection_status)


def test_urls(astore_client):
    """Catch potentially annoying and difficult to debug typos"""
    base_test_url = "http://{}:{}/".format(
        TESTING_CONFIG["host"], TESTING_CONFIG["port"]
    )
    astore_client._host_url == base_test_url
    astore_client.aheader_url == base_test_url + "analysis_header"
    astore_client.atail_url == base_test_url + "analysis_tail"
    astore_client.dref_url == base_test_url + "data_reference"
    astore_client.dref_header_url == base_test_url + "data_reference_header"


def test_doc_or_uid_to_uid(astore_client):
    m_uid = str(uuid.uuid4())
    test_dict = {"name": "test_doc", "uid": m_uid}
    m_uid == astore_client._doc_or_uid_to_uid(test_dict)


def test_post_fact(astore_client):
    pld = {"data": "bogus"}
    sig = "bogus"
    res = astore_client._post_factory(signature=sig, payload=pld)
    res["payload"] == pld
    res["signature"] == sig


def test_query_fact(astore_client):
    pld = {"data": "bogus"}
    sig = "bogus"
    res = astore_client._query_factory(signature=sig, payload=pld)
    res["payload"] == pld
    res["signature"] == sig


def test_header_insert(astore_server, astore_client):
    pytest.raises(TypeError, astore_client.insert_analysis_header)
    m_uid = str(uuid.uuid4())
    rid = astore_client.insert_analysis_header(
        uid=m_uid, time=time.time(), provenance={"version": 1.1}, custom=False
    )
    rid == m_uid


def generate_ahdr(astore_client):
    hid = astore_client.insert_analysis_header(
        uid=str(uuid.uuid4()),
        time=time.time(),
        provenance={"version": 1.1},
        custom=False,
    )
    return hid


def generate_dref(astore_client, hdr_id):
    did = astore_client.insert_analysis_tail(
        uid=str(uuid.uuid4()),
        analysis_header=hdr_id,
        time=time.time(),
        exit_status="test",
    )
    return did


def test_tail_insert(astore_client):
    pytest.raises(TypeError, astore_client.insert_analysis_tail)
    t_uid = str(uuid.uuid4())
    t = astore_client.insert_analysis_tail(
        uid=t_uid,
        analysis_header=generate_ahdr(astore_client),
        time=time.time(),
        exit_status="test",
    )
    t_uid == t


def test_dref_header_insert(astore_client):
    pytest.raises(TypeError, astore_client.insert_data_reference_header)
    dh_uid = str(uuid.uuid4())
    dh_id = astore_client.insert_data_reference_header(
        analysis_header=generate_ahdr(astore_client),
        time=time.time(),
        uid=dh_uid,
        data_keys={},
    )
    dh_id == dh_uid
