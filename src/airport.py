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
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()

# Getting output_data directory from the bash command inside the BashOperator
output_data = sys.argv[1]

# Read data from airport_i94port_join_table
df = spark.read.parquet(output_data + "airport_i94port_join_table_cleaned.parquet")

# Select the columns 
df_airport = df.select("ident", "type", "name", "i94port_code", "state_code", 'elevation_ft', 'longitude', 'latitude')

# Cahnge names of columns
names = ['airport_id', "airport_type", 'airport_name', 'i94port_code', 'state_code', 'elevation_ft', 'airport_longitude', 'airport_latitude']
df = df_airport.toDF(*names)

# Write airport_table to output_data directory
df_airport.write.parquet(output_data + "airport_table.parquet", 
        mode ="overwrite", partitionBy=["i94port_code"])

# print to the log
print("Write airport_table.parquet")