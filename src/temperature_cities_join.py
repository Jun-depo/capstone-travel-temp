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
from pyspark.sql.functions import udf, col, unix_timestamp, from_unixtime

import re
from geopy.distance import geodesic
import sys

def create_spark_session():
    """ Create a SparkSession to use Spark"""
    conf = SparkConf().set('spark.driver.memory', '6G') \
         .set('spark.executor.memory', "4G")   
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# importing the functions from helper.py caused error for unknown reason, copied 4 functions here to use directly. 
def remove_last2_from_colname(df):
    """
    Remove "2" if "2" is the last chracter of a column name.
    Input: Spark dataframe
    """
    new_colnames = []
    for i in df.columns:
        if i[-1]=="2":
            new_colnames.append(i[:-1])
        else:
            new_colnames.append(i) 
    df = df.toDF(*new_colnames)    
    return df

def convert_latitude(s):
    """
    Convert latitude to DD format. eg. 32.01N to 32.01, 32.01S to -32.01 for Latitude column from "Global_city_temperatures.csv"
    s: string from each row of the column
    Params:
        s: string containing latiitude data
    """
    if s[-1] == "N":
        return s[:-1]
    elif s[-1] == "S":
        return "-" + s[:-1]

def convert_longitude(s):
    """
    Convert longitude to DD format. eg. 100.01E to 100.01, 100.01W to -100.01 for Longitude column from "Global_city_temperatures.csv"
    Params:
        s: string containing longitude data
    """
    if s[-1] == "E": 
        return s[:-1]
    elif s[-1] == "W": 
        return "-"+ s[:-1]    

def change_data_type(col_lst, df, datatype):
    """
    change datatype of Spark df columns
    Params:
        col_lst: A list of column (names) with corresponding datatype need to be changed.
        df: Spark dataframe for the datatype change.  
        datatype: datatype needs to convert to
    """
    for i in col_lst:
        a = "cast(" + i + " as " + datatype + ")"+ i +'2'
        df = df.selectExpr("*", a).drop(i)
    return df


def us_cities_temperature_join_table(spark, input_data, output_data):
    """
    Create monthly_average_temperature_table for US cities
    Params:
        spark: SparkSession created by create_spark_session()
        input_data: input data directory, specified inside BashOperator (input_data = sys.argv[1]).
        output_data: output data directory, specified inside BashOperator (output_data = sys.argv[2])    
    """
    # read the global temperature table
    df_temp = spark.read.csv(input_data + "Global_city_temperatures.csv", header=True)
    # select only US cities
    df_usa_temp = df_temp.select("*").where(df_temp.Country=='United States')
    # drop two columns
    df_usa_temp = df_usa_temp.drop("AverageTemperatureUncertainty", "Country")

    # rename the columns 
    names = ['dt', 'average_temperature', 'city', 'latitude', 'longitude']
    df_usa_temp2= df_usa_temp.toDF(*names)
    
    # delete rows that containing null values in "average_temperature" column. 
    df_usa_temp2 = df_usa_temp2.where("average_temperature is not null")

    # udf functions for extracting year, month from year, month, from time data
    extract_year_udf = udf(lambda x: x[0:4])
    extract_month_udf = udf(lambda x: x[5:7])
    
    # extract year
    df_usa_temp3 = df_usa_temp2.withColumn("year", extract_year_udf("dt"))
    # extract month
    df_usa_temp3 = df_usa_temp3.withColumn("month", extract_month_udf("dt"))
    # convert date to datetime from unixtime
    df_usa_temp3 = df_usa_temp3.withColumn('date', from_unixtime(unix_timestamp('dt', 'yyyy-MM-dd')))

    # We select the most recent 10 years for averaing temperature.
    # setting up time range for averaging temperature. "2013-09-01" is the latest timestamp in the dataset.     
    dates = ("2003-09-01",  "2013-09-01")
    df_usa_temp_recent = df_usa_temp3.where(col("date").between(*dates))

    # change column data type to float
    df_usa_temp_recent = change_data_type(["average_temperature"], df_usa_temp_recent, "float")

    # change column data type to int
    df_usa_temp_recent = change_data_type(["year","month"], df_usa_temp_recent, "int")   
    # 2 was added to coulumn names after datatype transformation.  Delete 2 from the column names
    df_usa_temp_recent = remove_last2_from_colname(df_usa_temp_recent)
    
    # Calcualte average monthly temperatures for US cities.  Groupby: ["city", "month", "latitude", "longitude"]. Since city names are not unique and "state" data is not avaible in the dataset geo-location was used to as the co-selector of groupby.  Since average_temperatures are calculated for each month. "month" is also used for the groupby.
    df_usa_temp_recent_gb = df_usa_temp_recent.groupBy(["city", "month", "latitude", "longitude"]).mean('average_temperature')
    
    # rename the column "avg(average_temperature)" to 'average_temperature'
    df_usa_temp_recent_gb = df_usa_temp_recent_gb.withColumnRenamed("avg(average_temperature)", 'average_temperature')

    # convert latitude, longitude to signed degrees format (DDD.dddd)
    convert_lat = udf(lambda x: convert_latitude(x))
    df_usa_temp_recent_gb=df_usa_temp_recent_gb.withColumn("latitude", convert_lat("Latitude"))
    convert_lon = udf(lambda x: convert_longitude(x))
    df_usa_temp_recent_gb=df_usa_temp_recent_gb.withColumn("longitude", convert_lon("longitude"))

    # longitude and latitude data was correct in the "Global_city_temperatures.csv". The longitude and latitude of NYC maps NYC to New Jersey, Chicago to to Wisconsin (both incorrect). Get longitude and latitude data from external "uscities_simplemap.csv"
    df_sm = spark.read.csv(input_data + "uscities_simplemap.csv",  header=True)
    df_sm2 = df_sm.select("city", "state_id", "state_name", "lat", "lng")
    # lat and lng data map cities to the right states(tested separately)

    # Inner-join data from df_usa_temp_recent_gb and df_sm2
    df_comb = df_usa_temp_recent_gb.join(df_sm2, on="city")
    # change data type
    df_comb = change_data_type(["latitude", "longitude", "lat", "lng"], df_comb, "float")
    df_comb = remove_last2_from_colname(df_comb)

    # The above inner-join causes the cities with names merge with each other even they are in different states. To get rid of wrong join.  I calculated the distance from ("latitude", "longitude") of df_usa_temp_recent_gb and ("lat", "lng") of df_sm2 table
    km_distance_udf = udf(lambda lat1, lon1, lat2, lon2: geodesic((lat1, lon1), (lat2, lon2)).kilometers)
    df_comb2 = df_comb.withColumn("distance_km", km_distance_udf("latitude", "longitude", "lat", "lng"))
    df_comb2 = change_data_type(["distance_km"], df_comb2, "float")
    df_comb2 = remove_last2_from_colname(df_comb2)

    # applied selection filter to be < 180 km, that returns the eaxct the same numbers of rows as df_usa_temp_recent_gb
    df_comb3 = df_comb2.where(col("distance_km") < 180.0)
    df_comb4 = df_comb3.drop("latitude", "longitude", "distance_km")
    
    # Drop duplicates
    df_comb4 = df_comb4.dropDuplicates()
    # Export data in partitioned parquet format
    df_comb4.write.parquet(output_data + "us_city_temperatures_us_cities_join_table.parquet", 
        mode ="overwrite", partitionBy=['city', "month"])
    print("Write us_city_temperatures_us_cities_join_table.parquet")    

if __name__ == "__main__":
    # Getting input_data, output_data from Bash commmand inside BashOperator
    input_data = sys.argv[1]
    output_data = sys.argv[2] 
    
    spark = create_spark_session()
    us_cities_temperature_join_table(spark, input_data, output_data)
