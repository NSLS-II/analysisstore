from __future__ import (absolute_import, unicode_literals, print_function)
import requests
import ujson
from itertools import zip_longest
from .conf import host, port
import six
import time as ttime
from requests.exceptions import ConnectionError
from . import asutils



class AnalysisClient:
    """Client used to pass messages between analysisstore server and apps"""
    def __init__(self, host=host, port=port):
        self.host = host
        self.port = port
        self._insert_dict = {'analysis_header': self.insert_analysis_header,
                             'analysis_tail': self.insert_analysis_tail,
                             'data_reference_header': self.insert_data_reference_header,
                             'data_reference': self.insert_data_reference,
                             'bulk_data_reference': self.insert_bulk_data_reference}
        self._find_dict = {'analysis_header': self.find_analysis_header,
                           'analysis_tail': self.find_analysis_tail,
                           'data_reference_header': self.find_data_reference_header,
                           'data_reference': self.find_data_reference}

    @property
    def _host_url(self):
        """URL to the tornado instance"""
        return 'http://{}:{}/'.format(self.host, self.port)

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

    def connection_status(self):
        """Returns the connection status

        Returns
        -------
        bool
            Returns True if connected

        Raises
        -------
        tornado.web.HTTPError
            Raises 404 if no server to be found
        """
        r = requests.get(self._host_url + 'is_connected', timeout=0.1)
        r.raise_for_status()
        return True
    def _post_factory(self, payload, signature):
        return dict(payload=payload, signature=signature)

    def _query_factory(self, query, signature):
        return dict(query=query, signature=signature)

    def post(self, url, params):
        r = requests.post(url, data=ujson.dumps(params))
        r.raise_for_status()

    def insert_analysis_header(self, uid, time, provenance, **kwargs):
        """
        Create the entry point for analysis.

        Parameters
        ----------
        uid: str; optional
            Unique identifier for analysis_header document
        time: float; optional
            Time entry created. If not filled, server assigns a timestamp
        kwargs: dict
            Additional fields.

        Returns
        -------
        res: str
            uid of the document entered
        """
        payload = dict(uid=uid, time=time, provenance=provenance, **kwargs)
        params = self._post_factory(payload=payload, signature='insert_analysis_header')
        self.post(url=self.aheader_url, params=params)
        return uid

    def insert_analysis_tail(self, header, uid=None, time=None, exit_status=None, **kwargs):
        """
        Create analysis_tail document

        Parameters
        ----------
        header: doct.Document or uid
            analysis_header document this tail points to. Foreign key to the analysis_header.
        uid: str; optional
            Unique identifier for analysis_tail document. Server fills up this field if not provided
        time: float; optional
            Time document was created. Server fills up this field if not provided
        exit_status: str
            The status analysis stopped
        kwargs: dict
            Additional fields.
        Returns
        -------
        res: str
            uid of the inserted document
        """
        payload = dict(analysis_header=self._doc_or_uid_to_uid(header),
                       uid=uid, time=time ,
                       exit_status=exit_status if exit_status else "success",
                       **kwargs)
        res = asutils.post_document(url=self.atail_url, contents=payload)
        return res

    def insert_data_reference_header(self, header, uid=None, time=None, **kwargs):
        """
        Create data reference header document
        Parameters
        ----------
        header: doct.Document or uid
            analysis_header document this tail points to. Foreign key to the analysis_header.
        uid: str; optional
            Unique identifier for data_reference_header document. Server fills up this field if not provided
        time: float; optional
            Time document was created. Server fills up this field if not provided
        kwargs: dict
            Additional fields

        Returns
        -------
        res: str
            uid of the inserted document
        """
        payload = dict(analysis_header=self._doc_or_uid_to_uid(header),
                       uid=uid, time=time, **kwargs)
        res = asutils.post_document(url=self.dref_header_url, contents=payload)
        return res

    def insert_data_reference(self,  data_header, uid=None, time=None, **kwargs):
        payload = dict(data_reference_header=self._doc_or_uid(data_header),
                       uid=uid, time=time, **kwargs)
        res = asutils.post_document(url=self.dref_url, contents=payload)
        return res

    def insert_bulk_data_reference(self, data_header, data, chunk_size = 500, **kwargs):
        data_len = len(data)
        chunk_count = data_len // chunk_size + bool(data_len % chunk_size)
        chunks = self.grouper(data, chunk_count)
        for c in chunks:
            payload = ujson.dumps(list(c))
            asutils.post_document(url=self.dref_url, contents=payload)        

    def upload_file(self, header, file):
        """Upload one file at a time"""
        # I discourage and limit file upload to one file per time bc I do not want people
        # abusing this feature. Bulk inserts is as simple as passing a list of files
        files = {'files': open(file, 'rb')}
        # no metadata allowed, use header to store image info or image itself
        r = requests.post(self.fref_url, data={'header': self._doc_or_uid_to_uid(header)}, 
                          files=files, stream=True)
        r.raise_for_status()
    
    def get_file_names(self, header):
        """Returns a set of file names provided header information"""
        header = self._doc_or_uid_to_uid(header)
        r = requests.get(self.fref_url, params={'header': header})
        r.raise_for_status()
        content = ujson.loads(r.text)
        yield content
        
    def download_file(self, header, filename, display=False):
        header = self._doc_or_uid_to_uid(header)
        r = requests.get(self.fref_url, params={'header': header, 
                                                'filename': filename}, stream=True)
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=4096): 
                if chunk:
                    f.write(chunk)
        if display:
            from PIL import Image
            image = Image.open(filename)
            return image
        return filename # return the url to the local saved locally

    def update_analysis_header(self, query, update):
        raise NotImplementedError('Not sure if this is a good idea. Convince me that it is')

    def update_analysis_tail(self, query, update):
        raise NotImplementedError('Not sure if this is a good idea. Convince me that it is')

    def find_analysis_header(self, as_json=False, **kwargs):
        return asutils.get_document(url=self.aheader_url, doc_type='AnalysisHeader', 
                             as_json=as_json, contents=kwargs)

    def find_analysis_tail(self, as_json=False, **kwargs):
        return asutils.get_document(url=self.atail_url, doc_type='AnalysisTail', 
                             as_json=as_json, contents=kwargs)

    def find_data_reference_header(self, as_json=False, **kwargs):
        return asutils.get_document(url=self.dref_header_url, doc_type='DataReferenceHeader', 
                             as_json=as_json, contents=kwargs)

    def find_data_reference(self, as_json=False, **kwargs):
        return asutils.get_document(url=self.dref_url, doc_type='DataReference', 
                             as_json=as_json, contents=kwargs)
    
    def insert(self, doc_type, **kwargs):
        return self._insert_dict[doc_type](**kwargs)
        
    def find(self, doc_type, **kwargs):
        return self._find_dict[doc_type](**kwargs)
    
