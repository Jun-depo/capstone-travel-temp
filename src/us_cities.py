#!/usr/bin/env python3
# coding: utf-8

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext

import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col 
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

def us_cities_from_temp_cities_join_table(spark, input_data, output_data):
    
    """
    The function create us cities table from "us_city_temperatures_us_cities_join_table.parquet", 
    containing cities with temperature information availble in "USA_city_recent_average_monthly_temperatures.parquet" table   
    Params: 
        spark: SparkSession created by create_spark_session()
        input_data: input data directory, specified inside BashOperator (input_data = sys.argv[1]).
        output_data: output data directory, specified inside BashOperator (output_data = sys.argv[2])   
    """
    
    df=spark.read.parquet(output_data + "us_city_temperatures_us_cities_join_table.parquet")
    # Get columns for cities table
    cities = df.select("city", "state_id", "state_name","lat", "lng").dropDuplicates()
    # rename columns
    names = ['city', 'state_id', 'state_name', 'latitude', 'longitude']
    cities = cities.toDF(*names)
    # save cities as parquet files 
    cities.write.parquet(output_data + "USA_cities_from_temp.parquet", 
        mode ="overwrite", partitionBy=['city', 'state_id'])
    print("Write USA_cities_from_temp.parquet")

    
if __name__ == "__main__":
    # Getting input_data, output_data directories from Bash commmand inside airflow BashOperator.
    input_data = sys.argv[1]
    output_data = sys.argv[2]  
    
    
    spark = create_spark_session()
    us_cities_from_temp_cities_join_table(spark, input_data, output_data)
