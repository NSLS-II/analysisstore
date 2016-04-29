import ujson
import requests
from doct import Document

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
        raise ConnectionError('No AnalysisStore server found')
    r.raise_for_status() # this is for catching server side issue.
    return r.json()