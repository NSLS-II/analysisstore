from doct import Document
import time as ttime
import uuid
from analysisstore.client.conf import top_dir
from analysisstore.client.asutils import doc_or_uid_to_uid, read_from_json, write_to_json
from os.path import expanduser
import mongoquery
import json


def _find_local(fname, qparams, as_doct=False):
    """Find a document created using the local framework
    Parameters
    -----------
    fname: str
        Name of the query should be run
    qparams: dict
        Query parameters. Similar to online query methods

    Yields
    ------------
    c: doct.Document, StopIteration
        Result of the query if found

    """
    res_list = []
    try:
        with open(fname, 'r+') as fp:
            print(fname)
            local_payload = json.load(fp)
        qobj = mongoquery.Query(qparams)
        for i in local_payload:
            if qobj.match(i):
                res_list.append(i)
    except FileNotFoundError:
        raise RuntimeWarning('Local file {} does not exist'.format(fname))
    if as_doct:
        for c in res_list:
            yield Document(fname.split('.')[0], c)
    else:
        for c in res_list:
            yield c


def _update_local(fname, qparams, replacement):
    """Update a document created using the local framework
    Parameters
    -----------
    fname: str
        Name of the query should be run
    qparams: dict
        Query parameters. Similar to online query methods
    replacement: dict
        Fields/value pair to be updated. Beware of disallowed fields
        such as time and uid
    """
    try:
        with open(fname, 'r') as fp:
            local_payload = json.load(fp)
        qobj = mongoquery.Query(qparams)
        for _sample in local_payload:
            try:
                if qobj.match(_sample):
                    for k, v in replacement.items():
                        _sample[k] = v
            except mongoquery.QueryError:
                pass
        with open(fname, 'w') as fp:
            json.dump(local_payload, fp)
    except FileNotFoundError:
        raise RuntimeWarning('Local file {} does not exist'.format(fname))


class LocalAnalysisClient:
    def __init__(self, top_dir=top_dir):
        self.top_dir = expanduser(top_dir)
        self.aheader_list = read_from_json(self._aheader_url)
        self.atail_list = read_from_json(self._atail_url)
        self.dref_header_list = read_from_json(self._dref_header_url)
        self.dref_list = read_from_json(self._dref_url)

    @property
    def _aheader_url(self):
        return self.top_dir + '/analysis_header.json'

    @property
    def _atail_url(self):
        return self.top_dir + '/analysis_tail.json'

    @property
    def _dref_header_url(self):
        return self.top_dir + '/dref_header.json'

    @property
    def _dref_url(self):
        return self.top_dir + '/dref.json'

    def insert_analysis_header(self, uid, time, provenance, **kwargs):
        if 'container' in kwargs:
            kwargs['container'] = doc_or_uid_to_uid(kwargs['container'])
        doc = dict(uid=uid, time=time, provenance=provenance, **kwargs)
        self.aheader_list.append(doc)
        with open(self._aheader_url, 'w+') as fp:
            json.dump(self.aheader_list, fp)
        return doc

    def insert_analysis_tail(self, analysis_header, time, uid, **kwargs):
        doc = dict(uid=uid, time=time, header=analysis_header, **kwargs)
        self.atail_list.append(doc)
        with open(self._atail_url, 'w+') as fp:
            json.dump(self.atail_list, fp)
        return uid

    def insert_dref_header(self, analysis_header, time, uid, data_keys, **kwargs):
        doc = dict(uid=uid, time=time, header=analysis_header,
                   data_keys=data_keys, **kwargs)
        self.dref_header_list.append(doc)
        with open(self._dref_header_url, 'w+') as fp:
            json.dump(self.dref_header_list, fp)
        return uid

    def insert_dref(self, time, uid, dref_header):
        doc = dict(uid=uid, time=time, dref_header=dref_header)
        self.dref_list.append(doc)
        with open(self._dref_url, 'w+') as fp:
            json.dump(self.dref_list, fp)
        return uid

    def find_analysis_header(self, **kwargs):
        return _find_local(self._aheader_url, kwargs)

    def find_analysis_tail(self, **kwargs):
        return _find_local(self._atail_url, kwargs)

    def find_data_ref_header(self, **kwargs):
        return _find_local(self._dref_header_url, kwargs)

    def find_data_reference(self, **kwargs):
        return _find_local(self._dref_url, kwargs)


