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
    """Client used to pass messages between analysisstore server and apps"""
    def __init__(self, host='localhost:8999'):
        self.host = host #no need for port, provide one address
    
    @property
    def host(self):
        return self.__host
    
    @property 
    def _host_url(self):
        """URL to the"""
        return 'http://{}/'.format(self.host)
    
    @property
    def aheader_url(self):
        """URL for analysis header handler"""
        return self._host_url + 'analysis_header'
    
    @property
    def atail_url(self):
        """URL for analysis tail handler"""
        return self._host_url + 'analysis_tail'    
    
    @property
    def dref_url(self):
        """URL for data reference handler"""
        return self._host_url + 'data_reference'
    
    @property
    def dref_header_url(self):
        """URL for data reference header handler"""
        return self._host_url + 'data_reference_header'
        
    @property
    def fref_url(self):
        """URL for file upload handler"""
        return self._host_url + 'file'


    def _grouper(self, iterable, n, fillvalue=None):
        """Collect data into fixed-length chunks or blocks"""
        args = [iter(iterable)] * n
        return zip_longest(*args, fillvalue=fillvalue)
    
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
    
    def _validate_data_headers(self):
        pass
    
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
            r = requests.post(self.aheader_url, data=ujson.dumps(payload))
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
            r = requests.post(self.atail_url, data=ujson.dumps(payload))
        except ConnectionError:
            raise ConnectionError('No AnalysisStore server found')
        r.raise_for_status() # this is for catching server side issue.
        return payload

    def insert_data_reference_header(self, header, uid=None, time=None, as_doc=False, 
                             **kwargs):
        payload = dict(analysis_header=self._doc_or_uid(header),
                       uid=uid if uid else str(uuid.uuid4()), 
                       time=time if time else ttime.time(), **kwargs)
        try:
            r = requests.post(self.dref_header_url, data=ujson.dumps(payload))
        except ConnectionError:
            raise ConnectionError('No AnalysisStore server found')
        r.raise_for_status() # this is for catching server side issue.
        return payload

    def insert_data_reference(self,  data_header, uid=None, time=None, as_doc=False, 
                             **kwargs):
        self._validate_data_headers()
        payload = dict(data_reference_header=self._doc_or_uid(data_header),
                       uid=uid if uid else str(uuid.uuid4()), 
                       time=time if time else ttime.time(), **kwargs)
        try:
            r = requests.post(self.dref_url, data=ujson.dumps(payload))
        except ConnectionError:
            raise ConnectionError('No AnalysisStore server found')
        r.raise_for_status() # this is for catching server side issue.
        return payload

    def insert_bulk_data_reference(self, data_header, data, **kwargs):
        data_len = len(data)
        chunk_count = data_len // chunk_size + bool(data_len % chunk_size)
        chunks = self.grouper(data, chunk_count)
        for c in chunks:
            payload = ujson.dumps(list(c))
            r = requests.post(self.dref_url, data=c)        
        r.raise_for_status()

    def upload_file(self, header, file):
        """Upload """
        files = {'files': open(file, 'rb')}
        r = requests.post(self.fref_url, files=files, data={'header': header})
        r.raise_for_status()

    def update_analysis_header(self, query, update):
        pass

    def update_analysis_tail(self, query, update):
        pass

    def find_analysis_header(self, **kwargs):
        pass

    def find_analyis_tail(self, **kwargs):
        pass

    def find_data_reference_header(self, **kwargs):
        pass

    def find_data_reference(self, **kwargs):
        pass
