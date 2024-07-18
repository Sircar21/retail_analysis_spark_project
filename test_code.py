from pyspark.sql import SparkSession

def test_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("TestSparkSession") \
            .getOrCreate()
        print("Spark Session created successfully")
        spark.stop()
    except Exception as e:
        print("Error creating Spark Session", e)

if __name__ == "__main__":
    test_spark_session()
