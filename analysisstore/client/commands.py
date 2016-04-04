from __future__ import (absolute_import, unicode_literals, print_function)
import requests
import ujson
import jsonschema
from collections import deque
import uuid
import six
import time as ttime
from requests.exceptions import ConnectionError


class AnalysisClient:
    """Your path to AnalysisStore(yiiiiissssss, camelcase"""
    def __init__(self, host='localhost:8999'):
        self.host = host
        self.header_list = []
        self.data_ref_list = deque()
    
    @property
    def host(self):
        return self.__host
    
    @host.setter
    def host(self, new_val):
        self.__host = new_val
    
    @property 
    def _host_url(self):
        return 'http://{}/'.format(self.host)
    
    @property
    def aheader_url(self):
        return self._host_url + 'analysis_header'
    
    @property
    def atail_url(self):
        return self._host_url + 'analysis_tail'    
    
    @property
    def dref_url(self):
        return self._host_url + 'data_reference'
    
    @property
    def dref_header_url(self):
        return self._host_url + 'data_reference_header'
        
    def _doc_or_uid_to_uid(self, doc_or_uid):
    """Given Document or uid return the uid
    Parameters
    ----------
    doc_or_uid : dict or str
        If str, then assume uid and pass through, if not, return
        the 'uid' field
    Returns
    -------
    uid : str
        A string version of the uid of the given document
    """
    if not isinstance(doc_or_uid, six.string_types):
        doc_or_uid = doc_or_uid['uid']
    return str(doc_or_uid)
    
    def connection_status(self):
        """Check connection status"""
        try:        
            r = requests.get(self._host_url + 'is_connected')
        except ConnectionError:
            return False
        return True    
        
    def insert_analysis_header(self, analysis_header, uid=None, time=None, as_doc=False,
                               **kwargs):
        
        payload = dict(uid=uid if uid else str(uuid.uuid4()), 
                       time=time if time else ttime.time(), **kwargs)
        try:
            r = requests.post(self.aheader_url, params=ujson.dumps(payload))
        except ConnectionError:
            raise ConnectionError('No AnalysisStore server found')
        r.raise_for_status() # this is for catching server side issue.
        return payload
        
    def insert_analysis_tail(self, header, uid=None, time=None, as_doc=False, 
                             **kwargs):
        payload = dict(analysis_header=self._doc_or_uid(header),
                       uid=uid if uid else str(uuid.uuid4()), 
                       time=time if time else ttime.time(), **kwargs)
        try:
            r = requests.post(self.atail_url, params=ujson.dumps(payload))
        except ConnectionError:
            raise ConnectionError('No AnalysisStore server found')
        r.raise_for_status() # this is for catching server side issue.
        return payload

    def insert_data_reference(self, **kwargs):
        pass

    def insert_data_reference_header(self, header, uid=None, time=None, as_doc=False, 
                             **kwargs):
        payload = dict(analysis_header=self._doc_or_uid(header),
                       uid=uid if uid else str(uuid.uuid4()), 
                       time=time if time else ttime.time(), **kwargs)
        try:
            r = requests.post(self.dref_header_url, params=ujson.dumps(payload))
        except ConnectionError:
            raise ConnectionError('No AnalysisStore server found')
        r.raise_for_status() # this is for catching server side issue.
        return payload

    def create_bulk_data_reference(self, **kwargs):
        pass

    def update_analysis_header(self, **kwargs):
        pass

    def update_analysis_tail(self, **kwargs):
        pass

    def find_analysis_header(self, **kwargs):
        pass

    def find_analyis_tail(self, **kwargs):
        pass

    def find_data_reference_header(self, **kwargs):
        pass

    def find_data_reference(self, **kwargs):
        pass
