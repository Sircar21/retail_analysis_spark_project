import sys
import logging
from lib import DataManupulation, DataReader, utils, DataWriter
from pyspark.sql import SparkSession


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Starting the script")

def main(job_run_env):
    try:
        logger.info("Starting the main function")
        logger.info(f"Job run environment: {job_run_env}")
        
        logger.info("Creating the Spark Session")
        spark = utils.create_spark_session(job_run_env)

        logger.info("Spark Session created")
        logger.info(spark)

        logger.info("Reading orders data")
        orders_df = DataReader.read_orders(spark, job_run_env)
        logger.info("Filtering closed orders")
        orders_df = DataManupulation.filter_closed_orders(orders_df)

        logger.info("Reading customers data")
        customers_df = DataReader.read_customers(spark, job_run_env)

        logger.info("Joining orders and customers data")
        joined_df = DataManupulation.join_orders_customers(orders_df, customers_df)

        logger.info("Aggregating results")
        aggregated_results = DataManupulation.counts_orders_state(joined_df)
        aggregated_results.show()
        
        logger.info("Writing transfromed data to the output folder")
        print(DataWriter.writer_function(aggregated_results))


        logger.info("End of the main")
    except Exception as e:
        logger.error("An error occurred", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped")

if __name__ == "__main__":
    print("Entered to the applications pyspark_project")
    if len(sys.argv) < 2:
        logger.error("Please specify the environment")
        sys.exit(-1)
    
    job_run_env = sys.argv[1]
    print(f"Job run environment: {job_run_env}")
    main(job_run_env)


