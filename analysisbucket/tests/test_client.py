from analysisbucket.client import AnalysisClient, fill_result
from numpy.testing import assert_array_equal
from pandas.util.testing import assert_frame_equal

import numpy as np
import pandas as pd


def test_header_creation():
    ac = AnalysisClient()
    md = {k: ord(k) for k in 'abcdef'}
    h = ac.create_header('testing', uid='fixed', time=0, **md)

    for k, v in md.items():
        assert h[k] == v

    assert h.name == 'testing'
    assert h.uid == 'fixed'
    assert h.time == 0


def test_add_result():
    ac = AnalysisClient()

    md = {k: ord(k) for k in 'abcdef'}

    h = ac.create_header('testing', uid='fixed', time=0, **md)
    h = h.Document()

    tab = pd.DataFrame({'a': range(15), 'b': 'a'*15})
    arr = np.arange(15)

    res = ac.add_result(h, tab=tab, arr=arr)

    for d in [res['data'], res.descriptor.data_keys]:
        for k in ['tab', 'arr']:
            assert k in d
    res = fill_result(res)

    assert_array_equal(res.data['arr'], arr)
    assert_frame_equal(res.data['tab'], tab)


def test_HeaderDocument_add_result():
    ac = AnalysisClient()

    md = {k: ord(k) for k in 'abcdef'}

    h = ac.create_header('testing', uid='fixed', time=0, **md)

    res = h.add_result(a=1, b=2)

    for d in [res['data'], res.descriptor.data_keys]:
        for k in ['a', 'b']:
            assert k in d
    _name, res = res.to_name_dict_pair()
    res = fill_result(res)

    assert res['data']['a'] == 1
    assert res['data']['b'] == 2
