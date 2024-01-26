import ujson
import json
import requests
from doct import Document
import six


def get_document(url, doc_type, as_json, contents):
    r = requests.get(url, params=ujson.dumps(contents))
    r.raise_for_status()
    content = ujson.loads(r.text)
    if as_json:
        return r.text
    else:
        for c in content:
            yield Document(doc_type, c)


def post_document(url, contents):
    try:
        r = requests.post(url, data=ujson.dumps(contents))
    except ConnectionError:
        raise ConnectionError("No AnalysisStore server found")
    r.raise_for_status()  # this is for catching server side issue.
    return r.json()


def write_to_json(payload, filename):
    with open(filename, "w+") as fp:
        json.dump(payload, fp)


def read_from_json(filename):
    try:
        with open(filename, "r") as fp:
            return json.load(fp)
    except (FileNotFoundError, ValueError):
        return []


def doc_or_uid_to_uid(doc_or_uid):
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
        doc_or_uid = doc_or_uid["uid"]
    return str(doc_or_uid)
