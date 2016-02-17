
import tornado.ioloop
import tornado.web
from tornado import gen

import pymongo
import pymongo.errors as perr

import ujson
import jsonschema

from analysisbucket.server import utils


loop = tornado.ioloop.IOLoop.instance()


def db_connect(database, host, port):
    """Helper function to deal with stateful connections to motor.
    Connection established lazily.

    Parameters
    ----------
    database: str
        The name of database pymongo creates and/or connects
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server

    Returns pymongo.MotorDatabase
    -------
        Async server object which comes in handy as server has to juggle multiple clients
        and makes no difference for a single client compared to pymongo
    """
    client = pymongo.MongoClient(host=host, port=port)
    database = client[database]
    return database


class DefaultHandler(tornado.web.RequestHandler):
    """DefaultHandler which takes care of CORS for @hslepicka js gui. Does not hurt, one day we might need this"""
    @tornado.web.asynchronous
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Content-type', 'application/json')

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass


class AnalysisHeaderHandler(DefaultHandler):
    """Handler for analysis_header insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts

   Methods
    -------
    get()
        Query analysis_header documents. Query params are jsonified for type preservation
    post()
        Insert a analysis_header document.Same validation method as bluesky, secondary
        safety net.
    """

    @tornado.web.asynchronous
    def get(self):
        database = self.settings['db']
        query = utils.unpack_params(self)
        _id = query.pop('_id', None)
        find_last = query.pop('find_last', None)
        if _id:
            raise tornado.web.HTTPError(500, 'No ObjectId based search supported')
        if find_last:
            docs = database.analysis_header.find().sort('time', direction=pymongo.DESCENDING)
        else:
            docs = database.analysis_header.find(query)

        if not docs:
            raise tornado.web.HTTPError(500, reason='No results found for query')
        else:
            utils.return2client(self, docs)

    @tornado.web.asynchronous
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['analysis_header'])
        try:
            result = database.analysis_header.insert(data)
        except perr.PyMongoError:
            raise tornado.web.HTTPError(500,
                                        status='Unable to insert the document')

        database.analysis_header.create_index([('uid', pymongo.ASCENDING)],
                                       unique=True, background=True)
        database.analysis_header.create_index([('time', pymongo.ASCENDING)],
                                        unique=False)

        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils.return2client(self, data)