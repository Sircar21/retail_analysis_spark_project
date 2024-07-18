import pytest
from lib.utils import create_spark_session

@pytest.fixture
def spark():
    "For creating spark session"
    spark = create_spark_session("LOCAL")
    yield spark
    spark.stop()


@pytest.fixture
def expected_results(spark):
    "checking the expected results"
    schema = "state string, count int"
    return spark.read.format("csv").option("header",True).schema(schema).load("pyspark_etl_project/output_folder/*")

