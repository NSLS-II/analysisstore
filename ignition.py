from __future__ import (absolute_import, print_function, unicode_literals)
import argparse
import tornado.web
import tornado.ioloop
from  analysisstore.server.engine import (AnalysisHeaderHandler,
                                          AnalysisTailHandler)
from analysisstore.server.conf import load_configuration


def start_server(config=None):
    """
    Amostra service startup script.
    Returns tornado event loop provided configuration.

    Parameters
    ----------
    config: dict
        Command line arguments always have priority over local config or
        yaml files.Using these parameters,  a tornado event loop is created.
        Keep in mind that this server is started in lazy fashion. It does not verify
        the existence of a mongo instance running on the specified location.
    """
    if not config:
        config = {k: v for k, v in load_configuration('analysisstore', 'ASTR',
                                                      ['mongo_host', 'mongo_port', 'timezone',
                                                       'database', 'service_port'],
                                                      allow_missing=True).items() if v is not None}
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', dest='database', type=str,
                        help='name of database to use')
    parser.add_argument('--mongo_host',
                         dest='mongo_host', type=str,
                        help='host to use')
    parser.add_argument('--timezone', dest='timezone', type=str,
                        help='Local timezone')
    parser.add_argument('--mongo_port', dest='mongo_port', type=int,
                        help='port to use to talk to mongo')
    parser.add_argument('--service-port', dest='service_port', type=int,
                        help='port listen to for clients')
    args = parser.parse_args()
    if args.database is not None:
        config['database'] = args.database
    if args.mongo_host is not None:
        config['mongo_host'] = args.mongo_host
    if args.timezone is not None:
        config['timezone'] = args.timezone
    if args.mongo_port is not None:
        config['mongo_port'] = args.mongo_port
    service_port = args.service_port
    if service_port is None:
        service_port = 7770
    db = db_connect(config['database'],
                    config['mongo_host'],
                    config['mongo_port'])
    application = tornado.web.Application([
        (r'/header', AnalysisHeaderHandler),
        (r'/tail', AnalysisTailHandler)
         ], db=db)
    print('Starting Analysisstore service with configuration ', config)
    application.listen(service_port)
    tornado.ioloop.IOLoop.current().start()