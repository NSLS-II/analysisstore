from pathlib import Path
from uuid import uuid4

from analysisbucket.client import normalize, open_file


import numpy as np
from numpy.testing import assert_array_equal

import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_series_equal


def _norm_basic_helper(data):
    r, dk = normalize(data)
    assert dk == {}
    assert r == data


def test_norm_basic():
    for d in (1, 'abc', [1, 2, 3]):
        yield _norm_basic_helper, d


def _norm_np_helper(data):
    r, dk = normalize(data)
    assert isinstance(r, str)
    rd = open_file(r, dk)
    assert_array_equal(rd, data)
    assert dk['shape'] == data.shape
    assert dk['external'] == 'FILEPATH:npy'
    assert dk['dtype'] == 'array'


def test_norm_np():
    tests = [np.arange(5),
             np.linspace(0, 1, 20),
             np.arange(25).reshape(5, 5)]
    for d in tests:
        yield _norm_np_helper, d


def _norm_pdS_helper(data):
    r, dk = normalize(data)
    assert isinstance(r, str)
    rd = open_file(r, dk)
    assert dk['external'] == 'FILEPATH:csv'
    assert dk['dtype'] == 'table'
    # account for the to disk process upcasting to DataFrame
    assert dk['shape'] == data.shape + (1, )
    assert_series_equal(data, rd[data.name])


def _norm_pddf_helper(data):
    r, dk = normalize(data)
    assert isinstance(r, str)
    rd = open_file(r, dk)
    assert dk['external'] == 'FILEPATH:csv'
    assert dk['dtype'] == 'table'
    assert dk['shape'] == data.shape
    assert_frame_equal(data, rd)


def test_norm_pd():
    helper_mapper = {pd.DataFrame: _norm_pddf_helper,
                     pd.Series: _norm_pdS_helper}
    dd = pd.DataFrame({'a': range(15), 'b': 'a'*15})
    tests = [dd, dd['a']]

    for t in tests:
        yield helper_mapper[type(t)], t


def test_norm_path():
    p = Path('/tmp/{}.txt'.format(str(uuid4())))

    lorem_ipsum = """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
minim veniam, quis nostrud exercitation ullamco laboris nisi ut
aliquip ex ea commodo consequat. Duis aute irure dolor in
reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum."""

    with p.open(mode='w') as f:
        f.write(lorem_ipsum)

    r, dk = normalize(p)

    with open(r, 'r') as f:
        for inp, out in zip(lorem_ipsum.split('\n'), f):
            assert inp.strip() == out.strip()

    with open(r, 'r') as f1, p.open() as f2:
        for inp, out in zip(f1, f2):
            assert inp == out
