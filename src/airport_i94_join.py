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
from helper import remove_last2_from_colname, change_data_type
import re
import sys

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY'

""" Create a SparkSession to use Spark"""
conf = SparkConf().set('spark.driver.memory', '6G') \
     .set('spark.executor.memory', "4G")
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()

input_data = sys.argv[1]
output_data = sys.argv[2]    
 
i94data = output_data + "i94port_pd.csv"

"""   
Use Spark to create airport_i94port_join table from input "airport-codes_csv.csv"
and "./out/i94port_pd.csv" (which was created before airport_i94port_join table). 
This function extract, transform and clean data before saving tables in distributed parquet format.      
""" 

# Read airport table from "airport-codes_csv.csv". 
df_airport = spark.read.csv(input_data + "airport-codes_csv.csv", header=True)
# select only US airport 
df_air_usa = df_airport.where(col("iso_country")=="US")

# create udf function for longitude, latitude extraction
get_lon_udf = udf(lambda x: x.split(",")[0])
get_lat_udf = udf(lambda x: x.split(",")[1])

# extract longitude.
df_air_usa = df_air_usa.withColumn("longitude", get_lon_udf("coordinates"))
df_air_usa = df_air_usa.withColumn("latitude", get_lat_udf("coordinates"))

# Drop "coordinates" column
df_air_usa = df_air_usa.drop("coordinates")

# udf function for extracting 2 letter state code
get_state_udf = udf(lambda x: x[-2:])
# Extract state code from "iso_region" column
df_air_usa = df_air_usa.withColumn("state_code", get_state_udf("iso_region"))
df_air_usa = df_air_usa.drop("iso_region")

# select 'municipality' column do not contain null value.
df_air_usa = df_air_usa.where('municipality is not null')

# udf for low cases.  
low_case_udf = udf(lambda x: x.lower())
# Lowing cases in "municipality" column data
df_air_usa = df_air_usa.withColumn("municipality", low_case_udf("municipality"))

i94data = output_data + "i94port_pd.csv" #Read i94port created by 
df_i94port = spark.read.csv(i94data, header=True)

df_i94port = df_i94port.withColumn("city",low_case_udf("city"))

df_airport_i94port_join = df_air_usa.join(df_i94port, (df_air_usa.municipality == df_i94port.city)
                                        &(df_air_usa.state_code==df_i94port.state_code)).drop(df_i94port.state_code)
df_airport_i94port_join = df_airport_i94port_join.select("ident", "type", "name", "elevation_ft", "iso_country", 
    "municipality", "longitude", "latitude", "i94port_code", "state_name", "state_code", "city")

### Vast majority international travel occur at large airports.  Some Medium airports which contain "international" in their name,
### don't have international flights.
df_airport_i94port_join2_large = df_airport_i94port_join.filter((col("type")=="large_airport"))

float_colname_lst = ["elevation_ft", "longitude", "latitude"]
df_airport_i94port_join2_large = change_data_type(float_colname_lst, df_airport_i94port_join2_large, "float")
df_airport_i94port_join2_large = remove_last2_from_colname(df_airport_i94port_join2_large)

# JFK has "longitude" and "latitude" values at 0.  
### fill in values obtained from Wikipedia and Google search
replace_zero_long_udf = udf(lambda x: -73.778889 if x ==0 else x)
replace_zero_lat_udf = udf(lambda x: 40.639722 if x ==0 else x)

df_airport_i94port_join2_large = df_airport_i94port_join2_large.withColumn("longitude", replace_zero_long_udf("longitude"))
df_airport_i94port_join2_large = df_airport_i94port_join2_large.withColumn("latitude", replace_zero_lat_udf("latitude"))
df_airport_i94port_join2_large = df_airport_i94port_join2_large.fillna(13.12, "elevation_ft")

float_colname_lst2 = ["longitude", "latitude"]
df_airport_i94port_join2_large = change_data_type(float_colname_lst2, df_airport_i94port_join2_large, "float")
df_airport_i94port_join2_large = remove_last2_from_colname(df_airport_i94port_join2_large)

# Most i94port cities contain "International" in their airport names.
check_international_udf = udf(lambda x: "International" in x)
df_airport_i94port_join3 = df_airport_i94port_join2_large.where(check_international_udf('name')==True).dropDuplicates(["ident"])

# George Bush Intercontinental Houston Airport, John Wayne Airport, Eppley Airfield,  John Wayne Airport-Orange 
# County Airport and Detroit Metropolitan Wayne County Airport are international airports without "international" in their 
# airport_names among large airport. They will be combined with international airport before. William P Hobby Airport 
# and La Guardia Airport will not be included into final us_airport_i94port_join_table because immigration_table only 
# have data at city level, that would cause double (or multiple) counting for cities have multiple international airport.¶
sel_airport = (col("ident")=="KIAH") | (col("ident")=="KOMA") | (col("ident")=="KSNA") | (col("ident")=="KDTW")

df_airport_i94port_join4 = df_airport_i94port_join2_large.where(sel_airport).dropDuplicates(["ident"])

df_airport_i94port_join5 = df_airport_i94port_join3.union(df_airport_i94port_join4)

# Eliminate duplication by de-selecting the smaller airports for each cities to keep the larger airports.
# It can cause double or multiple counting when join with immigration table without this step. 
df_airport_i94port_join5 = df_airport_i94port_join5.where((col("ident")!="KLCK")&(col("ident")!="KMDW")&(col("ident")!="KSFB")&(col("ident")!="KBFI"))

df_airport_i94port_join5.write.parquet(output_data + "airport_i94port_join_table_cleaned.parquet", 
        mode ="overwrite", partitionBy=["i94port_code"])
print("Write airport_i94port_join_table_cleaned")




  
