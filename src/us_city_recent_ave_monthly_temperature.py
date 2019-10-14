
#!/usr/bin/env python3
# coding: utf-8

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext

import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
import sys


def create_spark_session():
    """ Create a SparkSession to use Spark"""
    conf = SparkConf().set('spark.driver.memory', '6G') \
         .set('spark.executor.memory', "4G")
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def us_cities_monthly_temperatures(spark, input_data, output_data):
    """
    The function create us cities monthly average temperature table.
    Params: 
        spark: SparkSession created by create_spark_session()
        input_data: input data directory, specified inside BashOperator (input_data = sys.argv[1]).
        output_data: output data directory, specified inside BashOperator (output_data = sys.argv[2])   
    """
    
    # Read data from "us_city_temperatures_us_cities_join_table.parquet"
    df=spark.read.parquet(output_data + "us_city_temperatures_us_cities_join_table.parquet")

    # select columns for this temperature table.
    df_temp = df.select("city", "state_id", "month", "average_temperature")
    
    # save temperature table as parquet files.  "us_city_temperatures_us_cities_join_table.parquet" table already dropped duplicates.
    df_temp.write.parquet(output_data + "USA_city_recent_average_monthly_temperatures.parquet", 
        mode ="overwrite", partitionBy=['city', "month"])
    print("Write USA_city_recent_average_monthly_temperatures.parquet")

if __name__ == "__main__":
    
    # Getting input_data, output_data directories from Bash commmand inside airflow BashOperator.
    input_data = sys.argv[1]
    output_data = sys.argv[2] 
    
    spark = create_spark_session()
    us_cities_monthly_temperatures(spark, input_data, output_data)

