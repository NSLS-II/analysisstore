import time as ttime
import uuid

import pytest
import subprocess
import sys
import uuid
from analysisstore.client.commands import AnalysisClient
import contextlib

testing_config = dict(
    host="localhost",
    port=7601,
    timezone="US/Eastern",
    service_port=7601,
    database="astoretest{0}".format(str(uuid.uuid4())),
    mongo_uri="mongodb://localhost",
    mongo_host="localhost",
    mongo_port=27017,
    testing=True,
    log_file_prefix="testing",
)


@contextlib.contextmanager
def astore_startup():
    ps = subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"from analysisstore.ignition import start_server; start_server(config={testing_config}) ",
        ],
    )
    ttime.sleep(1.3)  # make sure the process is started
    yield ps


@pytest.fixture(scope="session")
def astore_server():
    with astore_startup() as astore_fixture:
        yield


@pytest.fixture(scope="function")
def astore_client():
    c = AnalysisClient(
        {"host": testing_config["mongo_host"], "port": testing_config["service_port"]}
    )
    return c
