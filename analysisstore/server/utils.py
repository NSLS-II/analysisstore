from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.web
from pkg_resources import resource_filename as rs_fn
import ujson
import pymongo.cursor


SCHEMA_PATH = 'schemas'
SCHEMA_NAMES = {'analysis_header': 'analysis_header.json',
                'analysis_tail': 'analysis_tail.json',
                'data_reference': 'data_reference.json',
                'bulk_data_reference': 'bulk_data_reference.json',
                'data_reference_header': 'data_reference_header.json'}
fn = '{}/{{}}'.format(SCHEMA_PATH)
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn('analysisstore', resource_name=fn.format(filename))) as fin:
        schemas[name] = ujson.load(fin)

def _compose_err_msg(code, status, m_str=''):
    reason = status + str(m_str)
    return tornado.web.HTTPError(code, reason=reason )

def unpack_params(handler):
    """Unpacks the queries from the body of the header
    Parameters
    ----------
    handler: tornado.web.RequestHandler
        Handler for incoming request to collection

    Returns dict
    -------
        Unpacked query in dict format.
    """
    if isinstance(handler, tornado.web.RequestHandler):
        return ujson.loads(list(handler.request.arguments.keys())[0])
    else:
        raise TypeError("Handler provided must be of tornado.web.RequestHandler type")


def return2client(handler, payload):
    """Dump the result back to client's open socket. No need to worry about package size
    or socket behavior as tornado handles this for us

    Parameters
    -----------
    handler: tornado.web.RequestHandler
        Request handler for the collection of operation(post/get)
    payload: dict, list
        Information to be sent to the client
    """

    
    if isinstance(payload, pymongo.cursor.Cursor):
            l = []
            for p in payload:
                del(p['_id'])
                l.append(p)           
            handler.write(ujson.dumps(l))
    elif isinstance(payload, dict):
        del(payload['_id'])
        handler.write(ujson.dumps(list(payload)))
    else:
        handler.write('[')
        d = next(payload)
        while True:
            try:
                del(d['_id'])
                handler.write(ujson.dumps(d))
                d = next(payload)
                handler.write(',')
            except StopIteration:
                break
        handler.write(']')
    handler.finish()                                                                                                                                                                 