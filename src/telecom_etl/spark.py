"""One place to build the SparkSession, so tests and the CLI run the same config."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(
    app: str = "telecom-etl", master: str = "local[2]", shuffle_partitions: int = 4
) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .master(master)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Overwrite only the partitions a write touches. This is what makes
        # re-running a batch, or a late record landing in an old day, safe.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Keep partition columns as the strings they were written as; otherwise
        # event_date reads back as a date and batch as an int, depending on luck.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
