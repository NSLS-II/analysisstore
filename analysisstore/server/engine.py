from __future__ import (absolute_import, print_function)
from tornado import gen
import tornado.ioloop
import tornado.web
import pymongo
import os
import ujson
import json
from .utils import unpack_params
import doct
import types


loop = tornado.ioloop.IOLoop.instance()


class DefaultHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.astore = self.settings['astore']

    @gen.coroutine
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Content-type', 'application/json')

    def load_query(self):
        return unpack_params(self)

    def load_data(self):
        return ujson.loads(self.request.body.decode("utf-8"))

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly.
        Useful for streaming client"""
        pass

    def report_error(self, code, status, mstr=''):
        fmsg = str(status) + ' ' + str(mstr)
        raise tornado.web.HTTPError(status_code=code, reason=fmsg)

    @gen.coroutine
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
        docs_gen = func(**payload)
        if isinstance(docs_gen, (doct.Document, list, dict)):
            self.write(json.dumps(docs_gen))
        elif isinstance(docs_gen, types.GeneratorType):
            self.write(json.dumps(list(docs_gen)))
        self.finish()

    @gen.coroutine
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
        self.write(ujson.dumps({'status': True}))
        self.finish()

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


class ConnStatHandler(DefaultHandler):
    @gen.coroutine
    def get(self):
        self.finish()


class AnalysisHeaderHandler(DefaultHandler):
    """Handler for analysis_header insert, query, and update operations.
    No deletes are supported.

   Methods
    -------
    get()
        Query analysis_header documents.
        Query params are jsonified for type preservation so pure string
        query methods will not work
    post()
        Insert analysis_header documents.
    """
    def initialize(self):
        # Extends tornado specific handler
        self.astore = self.settings['astore']
        self.insertables = {'insert_analysis_header': self.astore.insert_analysis_header}
        self.queryables = {'find_analysis_header': self.astore.find_analysis_header}


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

    def initialize(self):
        # Extends tornado specific handler
        self.astore = self.settings['astore']
        self.insertables = {'insert_analysis_tail': self.astore.insert_analysis_tail}
        self.queryables = {'find_analysis_tail': self.astore.find_analysis_tail}


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
    @gen.coroutine
    def initialize(self):
        self.astore = self.settings['astore']
        self.insertables = dict(insert_data_reference_header=self.astore.insert_data_reference_header)
        self.queryables = {'find_data_reference_header': self.astore.find_data_reference_header}


class DataReferenceHandler(DefaultHandler):
    """Handler for event insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    Methods
    -------
    get()
        Query event documents. Get params are json encoded in order to
        preserve type
    post()
        Insert a event document.Same validation method as bluesky, secondary
        safety net.
    """
    @gen.coroutine
    def initialize(self):
        self.astore = self.settings['astore']
        self.insertables = dict(insert_data_reference=self.astore.insert_data_reference,
                                bulk_data_reference_insert=self.astore.bulk_data_reference_insert)
        self.queryables = {'find_data_reference': self.astore.find_data_reference}
