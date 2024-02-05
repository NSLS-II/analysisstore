import time as ttime
import uuid

import pytest
import subprocess
import sys
import uuid
from analysisstore.client.commands import AnalysisClient
import contextlib

testing_config = dict(
    timezone="US/Eastern",
    port=7601,
    database="astoretest{0}".format(str(uuid.uuid4())),
    mongo_uri="mongodb://localhost",
    host="localhost",
    testing=True,
)


@contextlib.contextmanager
def astore_startup():
    try:
        ps = subprocess.Popen(
            [
                sys.executable,
                "-c",
                f"from analysisstore.ignition import start_server; start_server(config={testing_config}) ",
            ],
        )
        ttime.sleep(1.3)  # make sure the process is started
        yield ps
    finally:
        ps.terminate()

@pytest.fixture(scope="session")
def astore_server():
    with astore_startup() as astore_fixture:
        yield


@pytest.fixture(scope="function")
def astore_client():
    c = AnalysisClient(
        {"host": testing_config["host"], "port": testing_config["port"]}
    )
    return c
