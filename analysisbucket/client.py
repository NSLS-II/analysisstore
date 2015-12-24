from functools import singledispatch
import numpy as np
import pandas as pd
from uuid import uuid4
import os
import os.path
import pathlib
import shutil
from time import time as ttime

from document import Document
BASE_PATH = os.path.expanduser('~/.cache/ab')


class AnalysisClient:
    def __init__(self, *, norm=None):
        if norm is None:
            norm = normalize
        self._normalize = norm

    def create_header(self, name, **kwargs):
        """Create an analysis Header

        Create the header for an analysis run.

        The schema for this needs to be filled in.

        Parameters
        ----------
        name : str
            Name of the analysis pipeline that this header is
            recording

        Returns
        -------
        a_head : doc
           Document (dict) representing the header
        """
        # TODO talk to database
        return Document('result_header',
                        {'date': ttime(),
                         'uid': str(uuid4()),
                         'name': name,
                         **kwargs})

    def add_result(self, head, **data):
        """Add results to a header

        Auto-magically generate result documents to go with
        the given header.  All key word arguments are bundled into
        the result Document.

        Parameters
        ----------
        head: Document
            Document of the header to attach the result to
        data: type
            description

        Returns
        -------
        result : Document
        """
        data_dict = {}
        data_keys = {}
        for k, v in data.items():
            nd, kd = self._normalize(v)
            data_dict[k] = nd
            data_keys[k] = kd

        desc = self.add_result_descriptor(head, data_keys, auto_gen=True)

        res = self.add_result_document(desc, data_dict)
        return res

    def add_result_document(self, descriptor, data_doc):
        """Low-level add result

        This method assumes you have done most of the hard work (ex
        dealing with filestore) your self.

        Parameters
        ----------
        descriptor : Document
            The result descriptor that describes the document

        data_doc : Document
            A (almost?) full result document.

        Returns
        -------
        res : Document
            The actual document inserted into the database

        """
        # TODO talk to database
        return Document('result', {'data': data_doc,
                                   'descriptor': descriptor,
                                   'uid': str(uuid4())})

    def add_result_descriptor(self, head, data_keys, **kwargs):
        """Create an result descriptor

        """
        # TODO talk to database
        return Document('res_desc', {'header': head,
                                     'data_keys': data_keys,
                                     'uid': str(uuid4()),
                                     **kwargs})


@singledispatch
def normalize(data):
    """normalize data for storage

    Parameters
    ----------
    data : object
       The data to be stored

    Returns
    -------
    data : object
       Normalized data to be shoved into the document store (eg mongo)

    data_key : dict
       Entry for the descriptor document

    """
    return data, {}


@normalize.register(np.ndarray)
def _norm_np(data):
    os.makedirs(BASE_PATH, exist_ok=True)
    fname = os.path.join(BASE_PATH,  str(uuid4()) + '.npy')
    np.save(fname, data)
    return fname, {'shape': data.shape,
                   'dtype': 'array',
                   'external': 'FILEPATH:npy'}


@normalize.register(pd.DataFrame)
def _norm_pd_df(data):
    os.makedirs(BASE_PATH, exist_ok=True)
    fname = os.path.join(BASE_PATH,  str(uuid4()) + '.csv')
    data.to_csv(fname)
    return fname, {'shape': data.shape,
                   'dtype': 'table',
                   'external': 'FILEPATH:csv',
                   'columns': list(data.columns)}


@normalize.register(pd.Series)
def _norm_pd_S(data):
    return normalize(pd.DataFrame(data))


@normalize.register(pathlib.Path)
def _norm_path(data):
    os.makedirs(BASE_PATH, exist_ok=True)
    old_name, ext = os.path.splitext(data.name)
    fname = os.path.join(BASE_PATH, str(uuid4()) + ext)
    data = data.absolute()
    shutil.copy2(data.as_posix(), fname)
    return fname, {'shape': (),
                   'dtype': 'path'}


def open_file(data, key_desc):
    """A quick and dirty FS stand-in

    This handles the case where:
      - exactly one datum per file
      - no parameters other than the handler and filename
        are required

    Parameters
    ----------
    data : str
        The data to open as a file

    key_desc : dict
        The data_key entry for this value

    Returns
    -------
    data : object
        However the file should be interpreted
    """
    handler_map = {'csv': lambda fname: pd.read_csv(fname, index_col=0),
                   'npy': np.load}

    klass, ext = key_desc['external'].split(':')
    assert klass == 'FILEPATH'
    return handler_map[ext](data)
