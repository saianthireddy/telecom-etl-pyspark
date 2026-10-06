import os
import time

import pytest

from telecom_etl.generate import generate
from telecom_etl.pipeline import run
from telecom_etl.spark import get_spark

# Python hands naive datetimes to Spark in the machine's local zone; the
# pipeline itself only parses strings, but the hand-built test rows do not.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = get_spark("telecom-etl-tests", master="local[2]", shuffle_partitions=2)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    """Three days generated with the README's defaults and run once."""
    root = tmp_path_factory.mktemp("lake")
    manifest = generate(root, batches=3)
    reports = run(spark, root)
    return root, manifest, reports
