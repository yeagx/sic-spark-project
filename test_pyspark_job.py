import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Eve", 10.0)], ["name", "amount"])
    result = clean_data(df).collect()
    assert sorted(r["name"] for r in result) == ["Alice", "Eve"]


def test_amount_less_or_equal_zero_removed(spark):
    df = spark.createDataFrame(
        [("Alice", 100.0), ("Bob", 0.0), ("Dana", -20.0)], ["name", "amount"]
    )
    result = clean_data(df).collect()
    assert [r["name"] for r in result] == ["Alice"]


def test_null_names_removed(spark):
    df = spark.createDataFrame([("Alice", 100.0), (None, 50.0)], ["name", "amount"])
    result = clean_data(df).collect()
    assert [r["name"] for r in result] == ["Alice"]


def test_amount_with_tax_is_calculated(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Eve", 10.0)], ["name", "amount"])
    result = {r["name"]: r["amount_with_tax"] for r in clean_data(df).collect()}
    assert result["Alice"] == pytest.approx(120.0)
    assert result["Eve"] == pytest.approx(12.0)