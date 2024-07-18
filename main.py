import sys
from lib import DataManupulation, DataReader, utils, logger
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")

    spark = utils.create_spark_session(job_run_env)

    logger = Log4j(spark)

    logger.warn("Created Spark Session")

    orders_df = DataReader.read_orders(spark,job_run_env)

    orders_filtered = DataManupulation.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customers(spark,job_run_env)

    joined_df = DataManupulation.join_orders_customers(orders_filtered,customers_df)

    aggregated_results = DataManupulation.counts_orders_state(joined_df)

    aggregated_results.show(50)

    #print(aggregated_results.collect())

    logger.info("this is the end of main")