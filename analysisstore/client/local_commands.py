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
        raise NotImplementedError()

    def find(self, name, query):
        raise NotImplementedError()

    def update(self, name, query, replacement):
        raise NotImplementedError()

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



from metadatastore.mds import MDS, MDSRO
import metadatastore.conf
import doct
from collections import deque
from tqdm import tqdm

NEW_DATABASE = 'csx_dump'
OLD_DATABASE = 'csx_migrated'

def compare(o, n):
    try:
        assert o['uid'] == n['uid']
        if 'reason' in o and o['reason'] == '':
            d_o = dict(o)
            del d_o['reason']
            o = doct.Document('RunStop', d_o)
            print('Caught it')
        assert o == n
    except AssertionError:
        print(o)
        print(n)
        raise

old_config = dict(database=OLD_DATABASE,
                  host='localhost',
                  port=27017,
                  timezone='US/Eastern')
new_config = old_config.copy()

new_config['database'] = NEW_DATABASE

old = MDSRO(version=0, config=old_config)
new = MDS(version=1, config=new_config)

total = old._runstart_col.find().count()
old_starts = tqdm(old.find_run_starts(), unit='start docs', total=total,
                  leave=True)
new_starts = new.find_run_starts()
for o, n in zip(old_starts, new_starts):
    compare(o, n)

total = old._runstop_col.find().count()
old_stops = tqdm(old.find_run_stops(), unit='stop docs', total=total)
new_stops = new.find_run_stops()
for o, n in zip(old_stops, new_stops):
    compare(o, n)

descs = deque()
counts = deque()
total = old._descriptor_col.find().count()
old_descs = tqdm(old.find_descriptors(), unit='descriptors', total=total)
new_descs = new.find_descriptors()
for o, n in zip(old_descs, new_descs):
    d_raw = next(old._descriptor_col.find({'uid': o['uid']}))
    num_events = old._event_col.find({'descriptor_id': d_raw['_id']}).count()
    assert o == n
    descs.append(o)
    counts.append(num_events)

total = sum(counts)
with tqdm(total=total, unit='events') as pbar:
    for desc, num_events in zip(descs, counts):
        old_events = old.get_events_generator(descriptor=desc,
                                              convert_arrays=False)
        new_events = new.get_events_generator(descriptor=desc,
                                              convert_arrays=False)
        for ev in zip(old_events, new_events):
            assert o == n
        pbar.update(num_events)
