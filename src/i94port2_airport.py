#!/usr/bin/env python3
# coding: utf-8

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys

""" Create a SparkSession to use Spark"""
conf = SparkConf().set('spark.driver.memory', '6G') \
     .set('spark.executor.memory', "4G")
# conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()

# # Getting output_data directory from Bash commmand inside airflow BashOperator.
output_data = sys.argv[1] 

# Read data from "airport_i94port_join_table_cleaned.parquet"
df = spark.read.parquet(output_data + "airport_i94port_join_table_cleaned.parquet")

# Select columns and drop duplicates
df_i94port2 = df.select("i94port_code", "city", "state_code").dropDuplicates()

# Export data as a partioned parquet file.  
df_i94port2.write.parquet(output_data + "i94port2.parquet", mode ="overwrite", partitionBy=["i94port_code"])
print("Write i94port2.parquet")