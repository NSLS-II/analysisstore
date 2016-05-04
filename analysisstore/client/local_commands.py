from .asutils import Document
import time as ttime
import uuid
from .conf import top_dir
from .asutils import doc_or_uid_to_uid, read_from_json, write_to_json
from os.url import expanduser

# TODO: Import configuration


class LocalAnalysisClient:
    def __init__(self, top_dir=top_dir):
        self.top_dir = expanduser(top_dir)
        self.aheader_list = read_from_json(self._aheader_url)
        self.aheader_tail = read_from_json(self._atail_url)
        self.dref_header_list = read_from_json(self._dref_header_url)
        self.dref_list = read_from_json(self.dref_list)

    @property
    def _aheader_url(self):
        return self.top_dir + 'analysis_header'

    @property
    def _atail_url(self):
        return self.top_dir + 'analysis_tail'

    @property
    def _dref_header_url(self):
        return self.top_dir + 'dref_header'

    @property
    def _dref_url(self):
        return self.top_dir + 'dref'

    def insert(self, name, doc):
        pass

    def find(self, name, query):
        pass

    def update(self, name, query, replacement):
        pass

    def _insert_analysis_header(self, doc):
        if 'container' in doc:
            doc['container'] = doc_or_uid_to_uid(doc['container'])
        self.aheader_list.append(doc)
        write_to_json(self.aheader_list, self.aheader_list)
        return doc

    def _insert_analysis_tail(self, doc):
        pass

    def _insert_dref_header(self, doc):
        pass

    def _insert_dref(self, doc):
        pass