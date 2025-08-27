import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F


def scd2_merge(existing_df, incoming_df):
    """Simple SCD2 merge for test purposes."""
    join_keys = ["customer_id"]
    incoming = incoming_df.select(*join_keys, "customer_name", "valid_from")

    updated_existing = (
        existing_df.alias("e")
        .join(incoming.alias("i"), join_keys, "left")
        .withColumn(
            "valid_to",
            F.when(F.col("i.valid_from").isNotNull(), F.col("i.valid_from")).otherwise(
                F.col("e.valid_to")
            ),
        )
        .select("e.customer_id", "e.customer_name", "e.valid_from", "valid_to")
    )

    new_rows = incoming_df.withColumn("valid_to", F.lit("9999-12-31"))

    return updated_existing.unionByName(new_rows)


def test_scd2_merge_updates_valid_to():
    spark = SparkSession.builder.master("local[1]").appName("scd2-test").getOrCreate()
    existing_df = spark.createDataFrame(
        [(1, "Old Name", "2020-01-01", "9999-12-31")],
        ["customer_id", "customer_name", "valid_from", "valid_to"],
    )
    incoming_df = spark.createDataFrame(
        [(1, "New Name", "2021-01-01")],
        ["customer_id", "customer_name", "valid_from"],
    )

    result = scd2_merge(existing_df, incoming_df)
    data = {r.customer_name: r.valid_to for r in result.collect()}

    assert data["Old Name"] == "2021-01-01"
    assert data["New Name"] == "9999-12-31"
    spark.stop()
