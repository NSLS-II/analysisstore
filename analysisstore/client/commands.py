from __future__ import (absolute_import, unicode_literals, print_function)
import requests
import ujson
import jsonschema
from collections import deque


class AnalysisClient:
    """Your path to AnalysisStore(yiiiiissssss, camelcase"""
    def __init__(self, host='localhost:8999'):
        self.host = host
        self.header_list = []
        self.data_ref_list = deque()
        self._host_url = 'http://{}/'.format(self.host)
        
    def connection_status(self):
        """Check connection status"""
        r = requests.get(self._host_url + 'is_connected')
        r.raise_for_status()
        return r.text

    def create_analysis_header(self, **kwargs):
        pass

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
