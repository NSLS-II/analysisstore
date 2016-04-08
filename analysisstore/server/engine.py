from __future__ import (absolute_import, print_function)
import tornado.ioloop
import tornado.web
from tornado import gen

import pymongo
import pymongo.errors as perr

import ujson
import jsonschema

from analysisstore.server import utils


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
    """Handler for analysis_header insert, query, and update operations. No deletes are supported.
    
   Methods
    -------
    get()
        Query analysis_header documents. Query params are jsonified for type preservation so pure string
        query methods will not work
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
            raise utils._compose_err_msg(500, reason='No ObjectId based search supported')
        if find_last:
            docs = database.analysis_header.find().sort('time', 
                                direction=pymongo.DESCENDING)
        else:
            docs = database.analysis_header.find(query).sort('time',
                                                             direction=pymongo.DESCENDING)
        if not docs:
            raise utils._compose_err_msg(500,
                                        reason='No results found for query',
                                        data=query)
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
            raise utils._compose_err_msg(500,
                                        status='Unable to insert the document',
                                        data=data)
        database.analysis_header.create_index([('uid', pymongo.DESCENDING)],
                                       unique=True, background=True)
        database.analysis_header.create_index([('time', pymongo.DESCENDING)],
                                        unique=False)
        if not result:
            raise utils._compose_err_msg(500, status='No result for given query')
        else:
            utils.return2client(self, data)

class AnalysisTailHandler(DefaultHandler):
    """Handler for analysis_tail insert and query operations.

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
        if _id:
            raise utils._compose_err_msg(500, reason='No ObjectId based search supported')
        
        
        docs = database.analysis_tail.find(query).sort('time',
                                                        direction=pymongo.DESCENDING)
        if not docs:
            raise utils._compose_err_msg(500, 
                                        reason='No results found for query',
                                        data=query)
        else:
            utils.return2client(self, docs)

    @tornado.web.asynchronous
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['analysis_tail'])
        try:
            result = database.analysis_header.insert(data)
        except perr.PyMongoError:
            raise utils._compose_err_msg(500,
                                        status='Unable to insert the document',
                                        data=data)
        database.analysis_tail.create_index([('analysis_header', pymongo.DESCENDING)],
                                       unique=True, background=True)
        database.analysis_tail.create_index([('uid', pymongo.DESCENDING)],
                                       unique=True, background=True)
        database.analysis_tail.create_index([('time', pymongo.DESCENDING)],
                                        unique=False)
        if not result:
            raise utils._compose_err_msg(500, status='No result for given query')
        else:
            utils.return2client(self, data)


class DataReferenceHeaderHandler(DefaultHandler):
    """Handler for data_reference_header insert and query operations.
    
    Methods
    -------
    get()
        Query data_reference_header documents. Query params are jsonified for type preservation
    post()
        Insert a event_header document.Same validation method as bluesky, secondary
        safety net.
    """
    @tornado.web.asynchronous
    def get(self):
        database = self.settings['db']
        query = utils.unpack_params(self)
        _id = query.pop('_id', None)
        if _id:
            raise utils._compose_err_msg(500, reason='No ObjectId based search supported')        
        docs = database.data_reference_header.find(query).sort('time',
                                                      direction=pymongo.DESCENDING)
        if not docs:
            raise utils._compose_err_msg(500, 
                                        reason='No results found for query',
                                        data=query)
        else:
            utils.return2client(self, docs)
    
    @tornado.web.asynchronous
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['event_header'])
        try:
            result = database.analysis_header.insert(data)
        except perr.PyMongoError:
            raise utils._compose_err_msg(500,
                                        status='Unable to insert the document',
                                        data=data)
        database.event_header.create_index([('analysis_header', pymongo.DESCENDING)],
                                       unique=True, background=False)
        database.event_header.create_index([('uid', pymongo.DESCENDING)],
                                       unique=True, background=False)
        database.event_header.create_index([('time', pymongo.DESCENDING)],
                                        unique=False)
        if not result:
            raise utils._compose_err_msg(500, status='No result for given query')
        else:
            utils.return2client(self, data)
            
class DataReferenceHandler(DefaultHandler):
    """Handler for event insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    Methods
    -------
    get()
        Query event documents. Get params are json encoded in order to preserve type
    post()
        Insert a event document.Same validation method as bluesky, secondary
        safety net.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        query = utils.unpack_params(self)
        docs = database.data_reference.find(query)
        if not docs:
            raise utils._compose_err_msg(500, 'No results for given query', query)
        else:
            utils.return2client(self, docs)

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        if isinstance(data, list):
            jsonschema.validate(data, utils.schemas['bulk_data_reference'])
            bulk = database.data_reference.initialize_unordered_bulk_op()
            for _ in data:
                if _ is not None:
                    bulk.insert(_)
            try:
                bulk.execute()
            except pymongo.errors.BulkWriteError as err:
                raise utils._compose_err_msg(500, err)
            database.data_reference.create_index([('time', pymongo.DESCENDING),
                                         ('data_reference_header', pymongo.DESCENDING)])
            database.data_reference.create_index([('uid', pymongo.DESCENDING)], unique=True)
        else:
            jsonschema.validate(data, utils.schemas['data_reference'])
            result = database.data_reference.insert(data)
            if not result:
                raise utils._compose_err_msg(500)

    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise utils._compose_err_msg(404)

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise utils._compose_err_msg(404)

    class FileHandler(DefaultHandler):
        """Provides user the ability to upload/retrieve data over the wire"""
        def post(self):
            # TODO: Default to a directory for reading/writing files
            print(self.request.files)
            files = self.request.files['files']
            for xfile in files:
                # get the default file name
                file = xfile['filename']
                # the filename should not contain any "evil" special characters
                # basically "evil" characters are all characters that allows you to break out from the upload directory
                index = file.rfind(".")
                filename = file[:index].replace(".", "") + str(time.time()).replace(".", "") + file[index:]
                filename = filename.replace("/", "")
                # save the file in the upload folder
                with open(filename, "wb") as out:
                    # Be aware, that the user may have uploaded something evil like an executable script ...
                    # so it is a good idea to check the file content (xfile['body']) before saving the file
                    out.write(xfile['body']) 

