""" Startup script for the server."""
#TODO: Replace this with a better startup mechanism
import tornado.web
from analysisstore.server.engine import (AnalysisHeaderHandler, loop, db_connect)
from metadataservice.server.conf import connection_config


db = db_connect(connection_config['database'],
                connection_config['host'],
                connection_config['port'])

if __name__ == "__main__":
    # start server in main thread
    application = tornado.web.Application([(r'/analysis_header',
                                            AnalysisHeaderHandler)], db=db)
    application.listen(7771)
    loop.start()