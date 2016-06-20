from pymongo import MongoClient, DESCENDING
import jsonschema
import json

class AStore:
    def __init__(self, config):
        self.client = MongoClient(host=config['host'],
                             port=config['port'])
        self.database = self.client[config['database']]
        self.database.analysis_header.create_index([('uid', DESCENDING)],
                                                    unique=True,
                                                   background=True)
        self.database.analysis_header.create_index([('time', DESCENDING)],
                                                   unique=False,
                                                   background=True)
        self.database.analysis_tail.create_index([('analysis_header',
                                                   DESCENDING)],
                                                 unique=True, background=True)
        self.database.analysis_tail.create_index([('uid', DESCENDING)],
                                                 unique=True, background=True)
        self.database.analysis_tail.create_index([('time', DESCENDING)],
                                                 unique=False, background=True)
        self.database.event_header.create_index([('analysis_header',
                                                  DESCENDING)],
                                                unique=True, background=False)
        self.database.event_header.create_index([('uid', DESCENDING)],
                                                unique=True, background=False)
        self.database.event_header.create_index([('time', DESCENDING)],
                                                unique=False)
        self.database.data_reference.create_index([('time', DESCENDING),
                                                  ('data_reference_header',
                                                   DESCENDING)])
        self.database.data_reference.create_index([('uid', DESCENDING)],
                                                  unique=True)

    def doc_or_uid_to_uid(self, doc_or_uid):
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
            self.doc_or_uid = doc_or_uid['uid']

    def extract_verify_ahdr(self, analysis_header):
        hdr = self.doc_or_uid_to_uid(analysis_header)
        if not next(self.find_analysis_header(uid=hdr)):
            raise RunTimeError('No Analysis Header found uid {}'.format(hdr))
        return hdr

    def extract_verify_dhdr(self, data_reference_header):
        hdr = self.doc_or_uid_to_uid(data_reference_header)
        if not next(self.find_data_reference_header(uid=hdr)):
            raise RunTimeError('No DataReferenceHeader found uid {}'.format(hdr))
        return hdr

    def insert_analysis_header(self, time, uid, provenance, **kwargs):
        doc = dict(time=time, uid=uid, provenance=provenance, **kwargs)
        self.database.analysis_header.insert(doc)
        return uid

    def insert_data_reference_header(self, time, uid, analysis_header,
                                     data_keys, **kwargs):
        hdr = self.extract_verify_ahdr(analysis_header)
        doc = dict(time=time, uid=uid, analysis_header=hdr, data_keys=data_keys,
                   **kwargs)
        self.database.data_reference_header.insert(doc)
        return uid

    def insert_data_reference(self, time, uid, data_reference_header,
                              data_keys, **kwargs):
        dhdr = self.extract_verify_dhdr(data_reference_header)
        doc = dict(time=time, uid=uid, data_reference_header=dhdr,
                   data_keys=data_keys, **kwargs)
        self.database.data_reference.insert(doc)
        return uid

    def insert_analysis_tail(self, time, uid, analysis_header, reason,
                             **kwargs):
        hdr = self.extract_verify_ahdr(analysis_header)
        doc = dict(time=time, uid=uid, analysis_header=hdr, data_keys=data_keys,
                   reason=reason, **kwargs)
        self.database.analysis_tail.insert(doc)
        return uid

    def find_analysis_header(self,  **kwargs):
        res = self.database.analysis_header.find(**kwargs)
        for r in res:
            yield r


    def find_data_reference_header(self, **kwargs):
        res = self.database.data_reference_header.find(**kwargs)
        for r in res:
            yield r

    def find_data_reference(self, **kwargs):
        return list(self.database.data_reference.find(**kwargs))

    def find_analysis_tail(self, **kwargs):
        res = self.database.analysis_tail.find(**kwargs)
        for r in res:
            yield r
