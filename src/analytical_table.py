#!/usr/bin/env python
# coding: utf-8

import findspark
findspark.init('/home/jun3/Downloads/spark-2.4.3-bin-hadoop2.7/')
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext

import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

import pandas as pd
import sys

def create_spark_session():
    """ 
    Create a SparkSession to use Spark. I increase memory in memory settings as default memory settings are 
    not sufficient for some processes. 
    """
    conf = SparkConf().set('spark.driver.memory', '6G') \
         .set('spark.executor.memory', "4G")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def analyze_temperature_entries(spark, input_data, output_data, mon_yr, year_month):
    """
    Great analytical tables from previosly created tables: "i94port2.parquet", "USA_city_recent_average_monthly_temperatures.parquet/" 
    and f"immigration_table_{mon_yr}.parquet".  {mon_yr} is extracted from execution_date corresponding a particular month of 
    immigration_entry data.
    Params:
        spark:  spark session created by create_spark_session()
        input_data: input data directory, specified inside BashOperator (input_data = sys.argv[1]).
        output_data: output data directory, specified inside BashOperator (output_data = sys.argv[2]).
        year_month: extracted from execution_date {{ ds }} of airflow (exe_date = sys.argv[3]).
    """
    df_imm_04 = spark.read.parquet(input_data + f"immigration_table_{mon_yr}.parquet") 

    # Study The Relationship Between Temperature and Port_entry
    df_i94port2 = spark.read.parquet(input_data + "i94port2.parquet")
    df_us_temp = spark.read.parquet(input_data + "USA_city_recent_average_monthly_temperatures.parquet/")

    # Join Immigration and i94 entry port tables
    df_imm_i94port= df_imm_04.join(df_i94port2, df_imm_04.i94port_city_code==df_i94port2.i94port_code, "inner")
    
    # Select columns 
    df_imm_i94port = df_imm_i94port.select("admission_number","i94port_city_code",  "visa_category_id", "entry_year",
                                                  "entry_month", "destination_state_code", "city", "state_code")
    # lower cases of city column for joining tables
    lower_case_udf=udf(lambda x: x.lower())
    df_us_temp = df_us_temp.withColumn("city", lower_case_udf("city"))
    
    # joining Imm_i94port table with temperature table on city, state_code (2 letters), and month 
    df_imm_i94port_temp = df_imm_i94port.join(df_us_temp, (df_imm_i94port.city == df_us_temp.city) & 
                                                     (df_imm_i94port.state_code == df_us_temp.state_id) &
                                                     (df_imm_i94port.entry_month == df_us_temp.month))


    df_imm_i94port_temp.count()/df_imm_i94port.count() # recoved 93.7% data after the table joining
    
    # Groupby by entry "i94port_city_code" columns and other columns ("city", "state_code", "average_temperature", 
    # "entry_month" are the same for the smae i94port_city_code) and count
    gb_entry_cities = df_imm_i94port_temp.groupBy(["i94port_city_code", df_imm_i94port.city, "state_code", 
                                                      "average_temperature", "entry_month"]).count()

    # Sorting the groupby dataframe in "count" descending order 
    gb_entry_cities = gb_entry_cities.sort("count", ascending=False)
    
    # Change to pandas df and export as a csv file with timestamp {year_month}
    gb_entry_cities.toPandas().to_csv(output_data + f"{year_month}_i94port_entry.csv")
    print(f"export analytical {year_month}_i94port_entry_table")

    # gb_entry_cities.toPandas().to_csv(output_data + f"{year_month}_i94port_entry.csv")

if __name__ == "__main__":
    # Getting input_data, output_data, execution_date from Bash commmand inside BashOperator
    input_data = sys.argv[1]
    output_data = sys.argv[2]     
    exe_date = sys.argv[3]
    
    #exe_date = "2016-04-01" ### for testing purpose without airflow executio_date {{ ds }}
    mon_yr = datetime.strptime(exe_date, '%Y-%m-%d').strftime('%b%y').lower()
    year_month = exe_date[0:7]
    
    # create spark session
    spark = create_spark_session()  
    # run the python script
    analyze_temperature_entries(spark, input_data, output_data, mon_yr, year_month)