from pymongo import MongoClient, DESCENDING
import pymongo
import jsonschema
import json
import six
from .utils import AnalysisstoreException


class AStore:
    def __init__(self, config, testing=False):
        """Given a database configuration that consists of uri and
        database, instantiate an AStore object that handles the connections
        to the database.

        Parameters
        -----------
        config: dict
            uri in string format, and database
        """
        if not testing:
            try:
                self.client = MongoClient(config["uri"])
                # Proactively check that connection to server is working.
                self.client.server_info()
            except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError):
                raise AnalysisstoreException("Unable to connect to MongoDB server...")
        else:
            import mongomock

            self.client = mongomock.MongoClient(config["uri"])
        self.database = self.client[config["database"]]

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
            doc_or_uid = doc_or_uid['uid']
        return doc_or_uid

    def extract_verify_ahdr(self, analysis_header):
        """Given an analysis header, verify it has been inserted
        and return its uid  whether it is an object or uid

        Parameters
        ----------
        analysis_header : doct.Document or str
            AnalysisHeader document or uid
        Returns
        -------
        str
            uid of the header that has been verified
        """
        hdr = self.doc_or_uid_to_uid(analysis_header)
        if not self.find_analysis_header(uid=hdr):
            raise RuntimeError('No Analysis Header found uid {}'.format(hdr))
        return hdr

    def extract_verify_dhdr(self, data_reference_header):
        """Given an analysis header, verify it has been inserted
        and return its uid  whether it is an object or uid

        Parameters
        ----------
        data_reference_header : doct.Document or str
            DataReferenceHeader document or uid
        Returns
        -------
        str
            uid of the data reference header that has been verified
        """
        hdr = self.doc_or_uid_to_uid(data_reference_header)
        if not self.find_data_reference_header(uid=hdr):
            raise RuntimeError('No DataReferenceHeader found uid {}'.format(hdr))
        return hdr

    def insert_analysis_header(self, time, uid, provenance, **kwargs):
        """Create a database entry for the analysis_header
        Parameters
        __________
        time : float
            Time of analysis header creation
         uid : str
            Unique identifier for the analysis header document
         provenance : dict
            Provenance information for this data analysis

        Returns
        --------
        str
            Unique identifier of the document inserted
        """
        doc = dict(time=time, uid=uid, provenance=provenance, **kwargs)
        self.database.analysis_header.insert_one(doc)
        return uid

    def insert_data_reference_header(self, time, uid, analysis_header,
                                     data_keys, **kwargs):
        """Create a database entry for the data_reference_header
        Parameters
        __________
        time : float
            Time of data reference header creation
         uid : str
            Unique identifier for the data reference header document
         analysis_header : doct.Document or uid
            Foreign key to data analysis header
        data_keys : dict
           Set of key/value pairs that describe the contents of data reference

        Returns
        --------
        str
            Unique identifier of the document inserted
        """
        try:
           hdr = self.extract_verify_ahdr(analysis_header)
        except RuntimeError:
           hdr = self.doc_or_uid_to_uid(analysis_header)
        doc = dict(time=time, uid=uid, analysis_header=analysis_header,
                   data_keys=data_keys,
                   **kwargs)
        self.database.data_reference_header.insert_one(doc)
        return uid

    def bulk_data_reference_insert(self, data_header, data_references):
        try:
            dhdr = self.extract_verify_dhdr(data_header)
        except RuntimeError:
            dhdr = self.doc_or_uid_to_uid(data_header)
        res = self.database.data_reference.insert_many(data_references,
                                                       ordered=False)
        print('inserted the bulk')
        return True

    def insert_data_reference(self, time, uid, data_reference_header,
                              data, timestamps, **kwargs):
        """
        Create data reference header document
        Parameters
        ----------
        data_header : doct.Document or uid
            data_reference_header document this tail points to. Foreign key to
            the data_reference_header.
        uid : str
            Unique identifier for data_reference document
        time : float
            Time document was created. Server fills up this field if not provided
        kwargs : dict
            Additional fields

        Returns
        -------
        uid : str
            uid of the inserted document
        """
        try:
            dhdr = self.extract_verify_dhdr(data_reference_header)
        except RuntimeError:
            dhdr = self.doc_or_uid_to_uid(data_reference_header)
        doc = dict(time=time, uid=uid, data_reference_header=dhdr,
                   data=data, timestamps=timestamps, **kwargs)
        self.database.data_reference.insert_one(doc)
        return uid

    def insert_analysis_tail(self, time, uid, analysis_header, exit_status,
                             **kwargs):
        """Create a database entry for the analysis_tail

        Parameters
        __________
        time: float
            Time of analysis tail creation
         uid: str
            Unique identifier for the analysis tail document
         analysis_header: doct.Document or uid
            Foreign key to data analysis tail

        Returns
        --------
        str
            Unique identifier of the document inserted
        """

        try:
            hdr = self.extract_verify_ahdr(analysis_header)
        except RuntimeError:
            hdr = self.doc_or_uid_to_uid(analysis_header)
        doc = dict(time=time, uid=uid, analysis_header=analysis_header,
                   exit_status=exit_status, **kwargs)
        self.database.analysis_tail.insert_one(doc)
        return uid

    def _clean_ids(self, cursor):
        # TODO: Replace this with mongo aggregation pipeline
        """Given a pymongo cursor, all _id fields are removed from set of documents"""
        res = []
        for c in cursor:
            del c['_id']
            res.append(c)
        return res

    def find_analysis_header(self,  **kwargs):
        """Given a set of parameters, return analysis header(s) that match the provided criteria"""
        projection = kwargs.pop('_projection', {})
        projection.update({"_id": False})
        cur = self.database.analysis_header.find(kwargs, projection=projection).sort([('time', DESCENDING),
                                                               ('uid', DESCENDING)])
        return list(cur)

    def find_data_reference_header(self, **kwargs):
        """Given a set of kwargs in mongo query format, returns a list of data
        reference headers that matches given criteria
        """
        cur = self.database.data_reference_header.find(kwargs).sort([('time', DESCENDING),
                                                                     ('uid', DESCENDING)])
        return self._clean_ids(cur)

    def find_data_reference(self, **kwargs):
        """Given a set of kwargs in mongo query format, returns a list of data
        reference that matches given criteria
        """
        cur = self.database.data_reference.find(kwargs).sort([('time', DESCENDING),
                                                                ('uid', DESCENDING)])
        return self._clean_ids(cur)

    def find_analysis_tail(self, **kwargs):
        """Given a set of kwargs in mongo query format, returns a list of data
        reference that matches given criteria
        """
        cur = self.database.analysis_tail.find(kwargs).sort([('time', DESCENDING),
                                                               ('uid', DESCENDING)])
        return self._clean_ids(cur)
