from __future__ import (absolute_import, unicode_literals, print_function)
import requests
import ujson
import jsonschema
from collections import deque
import uuid
import time as ttime

class AnalysisClient:
    """Your path to AnalysisStore(yiiiiissssss, camelcase"""
    def __init__(self, host='localhost:8999'):
        self.host = host
        self.header_list = []
        self.data_ref_list = deque()
        self._host_url = 
    
    @property
    def host(self):
        return __host
    
    @host.setter
    def host(self, new_val):
        self.__host = new_val
    
    @property 
    def _host_url(self):
        return 'http://{}/'.format(self.host)
    
    @property
    def aheader_url(self):
        return self._host_url + '/analysis_header'
    
    @property
    def atail_url(self):
        return self._host_url + '/analysis_tail'    
    
    @property
    def dref_url(self):
        return self._host_url + '/data_reference'
    
    @property
    def dref_header_url(self):
        return self._host_url + '/data_reference_header'
    
    def connection_status(self):
        """Check connection status"""
        r = requests.get(self._host_url + '/is_connected')
        status = False        
        if r.status_code == 200:
            status = True
        return status
        
    def create_analysis_header(self, uid=None, time=None, as_doc=False,
                               **kwargs):
        payload = dict(uid=uid if uid else str(uuid.uuid4()), 
                       time=time if time else ttime.time(), **kwargs)
        r = requests.get(self.aheader_url, params=ujson.dumps(payload))
        r.raise_for_status()
        return payload
        
    def create_analysis_tail(self, **kwargs):
        pass

    def create_data_reference(self, **kwargs):
        pass

    def create_data_reference_header(self, **kwargs):
        pass

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
