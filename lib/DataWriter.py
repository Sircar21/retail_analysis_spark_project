from pyspark.sql.functions import *

def writer_function(df):
    df.repartition(1).write.format("csv").option("header",True).mode("overwrite").option('path','pyspark_etl_project/output_folder').save()
    print("Analytics data has been written to the output folder")

    





