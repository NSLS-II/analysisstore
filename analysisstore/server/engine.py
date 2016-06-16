from __future__ import (absolute_import, print_function)
import tornado.ioloop
import tornado.web
from tornado import gen
import time
import pymongo
from pymongo import DESCENDING
import pymongo.errors as perr
import os
import ujson
import jsonschema
from analysisstore.server import utils


loop = tornado.ioloop.IOLoop.instance()



class DefaultHandler(tornado.web.RequestHandler):
    """DefaultHandler which takes care of CORS for @hslepicka js gui.
    Does not hurt, one day we might need this"""
    self.astore = self.settings['astore']
    @tornado.web.asynchronous
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Content-type', 'application/json')

    def load_query(self):
        return utils.unpack_params(self)

    def load_data(self):
        return ujson.loads(self.request.body.decode("utf-8"))

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass

    def report_error(self, code, status, mstr):
        fmsg = str(status) + ' ' + str(mstr)
        raise tornado.web.HTTPError(status_code=code, reason=fmsg)


class ConnStatHandler(DefaultHandler):
    @tornado.web.asynchronous
    def get(self):
        self.finish()


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
    self.insertables = {'insert_analysis_header': self.astore.insert_analysis_header}
    self.queryables = {'find_analysis_header': self.astore.find_analysis_header})

    def get_insertable(self, func):
        try:
            return self.insertables[func]
        except KeyError:
            self.report_error(500, 'Not a valid signature', func)

    def get_queryable(self, func):
        try:
            return self.queryables[func]
        except KeyError:
            self.report_error(500, 'Not a valid signature', func)

    @tornado.web.asynchronous
    def get(self):
        query = self.load_query()
        try:
            payload = query.pop('payload')
        except KeyError:
            self.report_error(400, 'No payload provided by the client')
        try:
            signature = query.pop('signature')
        except KeyError:
            self.report_error(400, 'No signature provided by the client')
        func = self.get_queryable(signature)
        res = func(**payload)
        self.return2client(res)
        self.finish()


   def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass

    @tornado.web.asynchronous
    def post(self):
        data = self.load_data()
        try:
            signature = data.pop('signature')
        except KeyError:
            self.report_error(400, 'No valid signature field provided')
        try:
            payload = data.pop('payload')
        except KeyError:
            self.report_error(400, 'A payload field must exist for post')
        func = self.get_insertable(signature)
        func(**payload)
        self.finish()



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
       pass 
    @tornado.web.asynchronous
    def post(self):
        pass   
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
                                                               direction=DESCENDING)
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
        jsonschema.validate(data, utils.schemas['data_reference_header'])
        try:
            result = database.analysis_header.insert(data)
        except perr.PyMongoError:
            raise utils._compose_err_msg(500,
                                        status='Unable to insert the document',
                                        data=data)
        if not result:
            raise utils._compose_err_msg(500, status='No result for given query')
        else:
            utils.return2client(self, data)

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass

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
    def get(self):
        database = self.settings['db']
        query = utils.unpack_params(self)
        docs = database.data_reference.find(query)
        if not docs:
            raise utils._compose_err_msg(500, 'No results for given query', query)
        else:
            utils.return2client(self, docs)

    @tornado.web.asynchronous
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        if isinstance(data, list):
            jsonschema.validate(data, utils.schemas['bulk_data_reference'])
            database.data_reference.insert_many(data)
        elif isinstance(data, dict):
            jsonschema.validate(data, utils.schemas['data_reference'])
            result = database.data_reference.insert(data)
        else:
            raise utils._compose_err_msg(403, 'Invalid document format')

    @tornado.web.asynchronous
    def put(self):
        raise utils._compose_err_msg(404, 'Data points cannot be updated')

    @tornado.web.asynchronous
    def delete(self):
        raise utils._compose_err_msg(404)

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass

class AnalysisFileHandler(DefaultHandler):
    """Provides user the ability to upload/retrieve data over the wire"""
    @property
    def save_path(self):
        return os.path.expanduser(self.settings['file_directory'])

    def manipulate_fname(self, file):
        index = file.rfind(".")
        filename = file[:index].replace(".", "") + file[index:]
        print(filename)
        filename = filename.replace("/", "")
        return filename

    def post(self):
        database = self.settings['db']
        files = self.request.files['files']
        header=self.get_argument("header", None, True)
        try:
            for xfile in files:
                # get the default file name
                file = xfile['filename']
                #refine evil characters that might mess with file directory of the server
                filename = self.manipulate_fname(file)
                # save the file in the upload folder
                _dir = "{}/{}".format(self.save_path, header)
                try:
                    os.mkdir(_dir, 755)
                except FileExistsError:
                    pass #neglect if header dir already created

                filenames = list(database.file_lookup.find({'analysis_header': header}).distinct('filename'))
                print(filenames)
                if filename in filenames:
                    raise utils._compose_err_msg(500, status='File already exists for header ',
                                                 m_str=header)
                full_fpath = os.path.join(_dir, filename)
                with open(full_fpath, "wb") as out:
                    # Make sure no executable whatsoever that might be insecure
                    out.write(xfile['body'])
                database.file_lookup.insert({'analysis_header': header, 'filename': filename})
                database.file_lookup.create_index([('analysis_header', pymongo.DESCENDING),
                                                   ('filename', pymongo.DESCENDING)], unique=False)
            self.finish()
        except:
            raise utils._compose_err_msg(500, 'Something went wrong saving the file')

    def get(self):
        database = self.settings['db']
        header=self.get_argument("header", None, True)
        filename = self.get_argument("filename", None, True)
        print(filename)
        if filename:
            try:
                f_doc = next(database.file_lookup.find({'analysis_header': header,
                                                        'filename': filename}))
            except StopIteration:
                raise utils._compose_err_msg(500, 'No such file saved for this header ')
            filename = f_doc['filename']
            _dir = "{}/{}".format(self.save_path, header) 
            print(_dir)
            _file_path = os.path.join(_dir, filename)
            if not filename or not os.path.exists(_file_path):
                raise utils._compose_err_msg(404, 'File does not exist on the server side')    
            with open(_file_path, "rb") as f:
                try:
                    while True:
                        _buffer = f.read(4096)
                        if _buffer:
                            self.write(_buffer)
                        else:
                            f.close()
                            self.finish()
                            return
                except:
                    raise utils._compose_err_msg(404)
            raise utils._compose_err_msg(500)
        else:
            f_list = []
            f_doc = database.file_lookup.find({'analysis_header': header})
            for f in f_doc:
                f_list.append(f['filename'])
            print(f_list)
            self.finish(ujson.dumps(f_list))

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass
