import configparser
from pyspark import SparkConf


#loading the applications configs in the python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("pyspark_etl_project/configs/application.conf")
    app_conf = {}
    for (key,value) in config.items(env):
        app_conf[key] = value
    return app_conf



# loading the pyspark config and creating the spark conf objects
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("pyspark_etl_project/configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf
