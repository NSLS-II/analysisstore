from pymongo import MongoClient, DESCENDING
import jsonschema
import json

class AStore:
    def __init__(self, config):
        client = MongoClient(host=config['host'],
                             port=config['port'])
        database.analysis_header.create_index([('uid', DESCENDING)],
                                              unique=True, background=True)
        database.analysis_header.create_index([('time', DESCENDING)],
                                              unique=False, background=True)
        database.analysis_tail.create_index([('analysis_header', DESCENDING)],
                                            unique=True, background=True)
        database.analysis_tail.create_index([('uid', DESCENDING)],
                                            unique=True, background=True)
        database.analysis_tail.create_index([('time', DESCENDING)],
                                            unique=False, background=True)
        database.event_header.create_index([('analysis_header', pymongo.DESCENDING)],
                                           unique=True, background=False)
        database.event_header.create_index([('uid', DESCENDING)],
                                           unique=True, background=False)
        database.event_header.create_index([('time', DESCENDING)],
                                           unique=False)
        database.data_reference.create_index([('time', DESCENDING),
                                             ('data_reference_header', DESCENDING)])
        database.data_reference.create_index([('uid', DESCENDING)], unique=True)

    def insert_analysis_header(self, time, uid, provenance, **kwargs):
        pass

    def insert_data_reference_header(self, time, uid, analysis_header, data_keys, **kwargs):
        pass

    def insert_data_reference(self, time, uid, data_ret=ference_header, data_keys, **kwargs):
        pass

    def insert_analysis_tail(self, time, uid, analysis_header, **kwargs):
        pass

    def find_analysis_header(self,  **kwargs):
        pass

    def find_data_reference_header(self, **kwargs):
        pass

    def find_data_reference(self, **kwargs):
        pass

    def find_analysis_tail(self, **kwargs):
        pass
