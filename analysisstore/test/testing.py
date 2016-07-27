from pymongo import MongoClient
import time as ttime
import uuid
import requests
import ujson
from subprocess import Popen
import os

TESTING_CONFIG = dict(host='localhost', port=7601,
                      timezone='US/Eastern', serviceport=7601,
                      database='astoretest')


def astore_setup():
    global proc
    f = os.path.dirname(os.path.realpath(__file__))
    proc = Popen(["python", "../../startup.py", "--mongo-host",
                  TESTING_CONFIG["mongohost",
                                 "--mongo-port",
                                 str(TESTING_CONFIG['mongoport']),
                                 "--database", TESTING_CONFIG['database'],
                                 "--timezone", TESTING_CONFIG['timezone'],
                                 "--service-port",
                                 str(TESTING_CONFIG['serviceport'])],cwd=f)
    print('Started the server with configuration..:{}'.format(TESTING_CONFIG))
    ttime.sleep(5) # make sure the process is started

def astore_teardown():
    proc2 = Popen(['kill', '-9', str(proc.pid)])
    ttime.sleep(5) # make sure the process is killed
    conn = MongoClient(host=TESTING_CONFIG['mongohost'], i
                       port=TESTING_CONFIG['mongoport'])
    conn.drop_database(TESTING_CONFIG['database'])
    ttime.sleep(2)
