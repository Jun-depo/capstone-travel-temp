#!/usr/bin/env python3
# coding: utf-8

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext

import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unix_timestamp, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import BooleanType

import pandas as pd
import numpy as np
import re
from helper import (clean_column_names, counting_null_number_inColumns, convert_latitude, 
convert_longitude, remove_last2_from_colname, change_data_type, remove_row_with_str, 
remove_end_comma_space, change_allowed_stay_date)
from helper import remove_extra_quote, extract_state, extract_city, extract_state2, extract_city2

from geopy.distance import geodesic

# To AWS access AWS 
# config = configparser.ConfigParser()
# config.read('dl.cfg')
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """ Create a SparkSession to use Spark"""
    conf = SparkConf().set('spark.driver.memory', '6G') \
        .set('spark.executor.memory', "4G")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def i94port_table(input_data, output_data):
    """
    The function is for creating i94port_table that contains city information for i94port entry_ciies.   
    It requires a lot of data cleaning.  The data size is very small. Pandas is good for cleaning this table 
    because of its flexibility. This table will merge with airport table that will create i94port2. 
    
    """
    
    # "i94port.txt" are from I94_SAS_Labels_Descriptions.SAS using text editor.
    df_i94port = pd.read_csv(input_data + "i94port.txt", sep="\t=\t", header=None, engine='python')
    
    # the data was double quotation makers. Remove one set. 
    df_i94port.loc[:,0] = df_i94port.loc[:,0].apply(remove_extra_quote)
    df_i94port.loc[:,1] = df_i94port.loc[:,1].apply(remove_extra_quote)
    # Rename the columns
    df_i94port.columns = ["i94port_code", "city_state"]
    
    # Select "city_state" column data to not contain "No PORT Code" and "Collapsed" as values
    df_i94port=df_i94port[df_i94port.loc[:,"city_state"].apply(lambda x: x[:12]!="No PORT Code" and x[:9]!="Collapsed")]
    
    # Create city columns by extracting city from "city_state" column.
    df_i94port["city"]=df_i94port.loc[:, "city_state"].apply(extract_city)
    
    # Create state columns by extracting state from "city_state" column.
    df_i94port["state"] = df_i94port.loc[:, "city_state"].apply(extract_state)
    
    # Remove "," from the previous extraction step. 
    df_i94port["city"] = df_i94port["city"].apply(lambda x: x[:-1] if x[-1]==',' else x)
    
    # The "state" column contains 2 letter state code.  Select length of the column data as 2.  
    df_i94port2a=df_i94port[df_i94port["state"].apply(lambda x: len(x)==2)]

    # clean the data len("state" column) don't equal 2.  "state" column contains 2 letter US state_codes. 
    df_i94port2b=df_i94port[df_i94port["state"].apply(lambda x: len(x)!=2)]
    
    # In these case, State data was not extracted into the state column in the previous steps. They need to be extracted by the sencond function extract_state2()
    df_i94port2b.loc[df_i94port2b["state"]=="(BPS)", "state"] = df_i94port2b.loc[df_i94port2b["state"]=="(BPS)", 
                                                                                "city"].apply(extract_state2)
    df_i94port2b.loc[df_i94port2b["state"]=="#ARPT", "state"] = df_i94port2b.loc[(df_i94port2b["state"]=="#ARPT"), 
                                                                                "city"].apply(extract_state2)
    
    # select data with len("state" column) equal to 2 after extract_state2() function extraction
    df_i94port2c = df_i94port2b[df_i94port2b["state"].apply(lambda x: len(x)==2)]
    
    # Also extract city with extract_city2 function
    df_i94port2c["city"] = df_i94port2c["city"].apply(extract_city2)

    # Further clean the data len("state") that are not 2. 'DIST. OF COLUMBIA' data is important, as it's a major port city for international entries into USA.
    df_i94port2d = df_i94port2b[df_i94port2b["state"].apply(lambda x: len(x)!=2)]
    df_i94port2d.loc[462, "city"]="DERBY LINE"
    df_i94port2d.loc[462, "state"] = "VT"
    df_i94port2d.loc[473, "state"] ="VT"
    df_i94port2d.loc[473, "city"] ="SWANTON"
    df_i94port2d.loc[428, "city"] = "PASO DEL NORTE"
    df_i94port2d.loc[428, "state"] = "TX"
    df_i94port2d.loc[478, "state"]="DC"
    df_i94port2d.loc[478, "city"]='DIST. OF COLUMBIA'

    ### select data len("state") equal to 2 after above steps.  The remaining len() not equaling to 2 data are foreign countries
    df_i94port2e = df_i94port2d[df_i94port2d["state"].apply(lambda x: len(x)==2)]

    ### combine all the useful dataframes from previous steps
    df_i94port_comb = pd.concat([df_i94port2a, df_i94port2c, df_i94port2e])

    # cleaning data where "city" column string data contain "/". Extract city data 
    df_i94port_comb.loc[119, "city"] = "IOWA CITY"
    df_i94port_comb.loc[208, "city"] = 'ST PAUL'
    df_i94port_comb.loc[258, "city"] = 'RALEIGH'
    df_i94port_comb.loc[293, "city"] = "NEWARK"
    df_i94port_comb.loc[307, "city"] = "RENO"

    # clean "city" column data that contains " - ". Extract city data after string.split() 
    df_i94port_comb["city"] = df_i94port_comb["city"].apply(lambda x: x.split(" - ")[1] if " - " in x else x)

    # Get state_code from "i94state_code.txt" for checking state_code in df_i94port_comb 
    df_states = pd.read_csv(input_data + "i94state_code.txt", sep="=", header=None)
    df_states.columns = ["state_code", "state_name"]
    df_states["state_code"] = df_states["state_code"].apply(remove_extra_quote)
    df_states["state_name"] = df_states["state_name"].apply(remove_extra_quote)

    # select columns for df_i94port_comb to create df_i94port_comb2.
    df_i94port_comb2 = df_i94port_comb[["i94port_code", "city", "state"]]
    # Rename the column
    names = ['i94port_code', 'city', 'state_code']
    df_i94port_comb2.columns = names

    # check state_code of df_i94port_comb2 to see if they are real state_code. The previous steps select length of string data as 2.
    # create state code list
    state_code_ls = set(df_states["state_code"])
    # print out 2 letter codes that are not in state code list.  "MX" and "5)" are not in the list. "MX" is Mexico. 
    for i in df_i94port_comb2["state_code"]:
        if i not in state_code_ls:
            print(i)

    # de-select data with "state_code" of "MX" (Mexico) 
    df_i94port_comb2 = df_i94port_comb2[df_i94port_comb2["state_code"]!="MX"]

    # clean the with "state_code" of "5)" 
    df5= df_i94port_comb2[df_i94port_comb2["state_code"] =="5)"]
    df_i94port_comb2.loc[463, "state_code"] ="VT"
    df_i94port_comb2.loc[463, "city"] ="DERBY LINE"

    # Merge df_i94port_comb2 and df_states to add "state_name" to the dataframe
    df_i94port_comb3 = df_i94port_comb2.merge(df_states, left_on="state_code", right_on="state_code")
    # Drop duplicates
    df_i94port_comb3 = df_i94port_comb3.drop_duplicates()

    df_i94port_comb3.to_csv(output_data + "i94port_pd.csv", index=False)
    print("Write i94port_pd table")


def airport_i94_join(spark, input_data, output_data):

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

    i94data = output_data + "i94port_pd.csv"
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

    
def airport(spark, output_data):
    """
    Generate airport table from airport_i94port_join_table.
    Params:
       output_data: output_data directory     
    
    """

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
    
    
def df_i94port2(spark, output_data):
    """
    Generate i94port2 table from airport_i94port_join_table.
    Params:
       output_data: output_data directory     
    
    """
    # Read data from "airport_i94port_join_table_cleaned.parquet"
    df = spark.read.parquet(output_data + "airport_i94port_join_table_cleaned.parquet")

    # Select columns and drop duplicates
    df_i94port2 = df.select("i94port_code", "city", "state_code").dropDuplicates()

    # Export data as a partioned parquet file.  
    df_i94port2.write.parquet(output_data + "i94port2.parquet", mode ="overwrite", partitionBy=["i94port_code"])
    print("Write i94port2.parquet")

    
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
    
def us_cities_from_temp_cities_join_table(spark, input_data, output_data):
    
    """
    The function create us cities table from "us_city_temperatures_us_cities_join_table.parquet", 
    containing cities with temperature information availble in "USA_city_recent_average_monthly_temperatures.parquet" table   
    Params: 
        spark: SparkSession created by create_spark_session()
        input_data: input data directory
        output_data: output data directory   
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
    
def us_cities_monthly_temperatures(spark, input_data, output_data):
    """
    The function create us cities monthly average temperature table.
    Params: 
        spark: SparkSession created by create_spark_session()
        input_data: input data directory.
        output_data: output data directory.   
    """
    
    # Read data from "us_city_temperatures_us_cities_join_table.parquet"
    df=spark.read.parquet(output_data + "us_city_temperatures_us_cities_join_table.parquet")

    # select columns for this temperature table.
    df_temp = df.select("city", "state_id", "month", "average_temperature")
    
    # save temperature table as parquet files.  "us_city_temperatures_us_cities_join_table.parquet" table already dropped duplicates.
    df_temp.write.parquet(output_data + "USA_city_recent_average_monthly_temperatures.parquet", 
        mode ="overwrite", partitionBy=['city', "month"])
    print("Write USA_city_recent_average_monthly_temperatures.parquet")
    
def immigration(spark, input_data, output_data, exe_date):
    """
    The function create immigration table that is the fact table that contains information for data analysis.
    Params: 
        spark: SparkSession created by create_spark_session()
        input_data: input data directory, specified inside BashOperator (input_data = sys.argv[1]).
        output_data: output data directory, specified inside BashOperator (output_data = sys.argv[2])   
        exe_date: execution_date {{ ds }} of airflow (exe_date = sys.argv[3]) 
    """    
    # Extract imm_date in month_year (apr16) format.  
    month_year = datetime.strptime(exe_date, '%Y-%m-%d').strftime('%b%y').lower()
    
    # Open i94 immigration table with correct month_year
    df_imm=spark.read.parquet(input_data + f'i94_{month_year}_sub.parquet/') 
    # delete columns that don't exist in data from other months. These columns are in July data, but not in some other month data
    df_imm= df_imm.drop('validres', 'delete_days', 'delete_mexl', 'delete_dup', 'delete_visa', 'delete_recdup')

    # convert SAS date to datetime date
    convert_to_dt_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    df_imm = df_imm.withColumn("arrival_date", convert_to_dt_date(df_imm.arrdate))
    df_imm = df_imm.withColumn("departure_date", convert_to_dt_date(df_imm.depdate))
    
    # Drop columns
    df_imm = df_imm.drop("arrdate", "depdate")
    
    # Delete rows with null values in "dtaddto" column
    df_imm = df_imm.where("dtaddto is not null")
    df_imm = df_imm.where(col("dtaddto")!="")
    
    # Create regular expression function to get rid of data with strange formats that could cause errors during later processing steps. The functionis not  perfect for completely select correct datetime dates.  But it's sufficient for the current purpose. Also,  preserve "D/S" data for "dtaddto" column.  Over 90% F1 (student visa) entries have "D/S" in "dtaddto" column.    
    def regex_filter(x):
        """
        The function label strange entries as False.  Correct datetime entries or "D/S" as True.    
        params: 
            x: a string containing date information
        """
        regexs = ['[01][0-9][0-3][0-9]20[12][6-9]', "D/S"]    
        if x:
            for r in regexs:
                if re.match(r, x, re.IGNORECASE):
                    return True    
        return False 

    # conver regex_filter() function to udf function for spark.
    regex_filter_udf = udf(regex_filter, BooleanType())

    # label True or False in "regEX_dtaddto" columns for "dtaddto" to detect if date is legit.  
    df_imm = df_imm.withColumn("regEX_dtaddto", regex_filter_udf("dtaddto"))
    # select rows col("regEX_dtaddto")==True
    df_imm2 = df_imm.where(col("regEX_dtaddto")==True)

    # Drop columns containing a lot of null values.  
    df_imm2 = df_imm2.drop("visapost", "occup", "entdepd", "entdepu", "insnum", "matflag", "regEX_dtaddto", "gender")
    
    # "dtadfile" only has 1 null value, remove it
    df_imm2 = df_imm2.select("*").where(("dtadfile is not null"))  
    
    #  Process "dtadfile" column, change column name to "add_to_file_date".
    change_dtadfil_foramt_udf = udf(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))
    df_imm2 = df_imm2.select("*").withColumn("add_to_file_date", change_dtadfil_foramt_udf(df_imm.dtadfile))
    df_imm2 = df_imm2.drop("dtadfile")
    
    # rename the "dtaddto" column.
    df_imm2 = df_imm2.withColumnRenamed("dtaddto", 'allow_to_stay_date')

    # change column names with names in the "names" list
    names =['cicid',
    'entry_year',
    'entry_month',
    'citizenship_of_country',
    'resident_of_country',
    'i94port_city',
    'entry_mode_id',
    'destination_state',
    "age",
    'visa_category_id',
    'count',
    'departure_flag',
    'birth_year',
    'allow_to_stay_date',
    'airline_code',
    'admission_number',
    'flight_number',
    'visa_type',
    'arrival_date',
    'departure_date',
    'add_to_file_date'
    ]
    
    df_imm2 = df_imm2.toDF(*names)

    int_lst = ['cicid',"admission_number", "entry_year", "entry_month", "citizenship_of_country", 
            "resident_of_country", "visa_category_id", "entry_mode_id", "age", "birth_year", "count"]
    
    # change datatype to integer for columns in int_lst.
    df_imm2 = change_data_type(int_lst, df_imm2, "int") 
    # remove last letter 2 from column name. 
    df_imm2 = remove_last2_from_colname(df_imm2)

    # Pre-selct columns with names in a list
    imm_colnames = ['cicid',
                    'admission_number',
                    'i94port_city',
                    'destination_state',
                    'allow_to_stay_date',
                    'airline_code',
                    'flight_number',
                    'visa_type',
                    'arrival_date',
                    'departure_date',
                    'add_to_file_date',
                    'entry_year',
                    'entry_month',
                    'citizenship_of_country',
                    'resident_of_country',
                    'visa_category_id',
                    'entry_mode_id',
                    'age']

    # select the columns
    df_imm3 = df_imm2.select(imm_colnames)

    # rename columns
    imm_colnames = ['cicid',
                    'admission_number',
                    'i94port_city_code',
                    'destination_state_code',
                    'allow_to_stay_date',
                    'airline_code',
                    'flight_number',
                    'visa_type',
                    'arrival_date',
                    'departure_date',
                    'add_to_file_date',
                    'entry_year',
                    'entry_month',
                    'citizenship_of_country_id',
                    'resident_of_country_id',
                    'visa_category_id',
                    'entry_mode_id',
                    'age']

    df_imm3 = df_imm3.toDF(*imm_colnames)
    # drop rows with null values
    df_imm3 = df_imm3.dropna()
    # Drop duplicates
    df_imm3 = df_imm3.dropDuplicates(['cicid'])

    # # Export data in partitioned parquet format. 
    df_imm3.write.parquet(output_data + f"immigration_table_{month_year}.parquet", mode ="overwrite", 
        partitionBy=['entry_month', "i94port_city_code", "destination_state_code"])    
    print(f"Write_immigration_table_{imm_date}.parquet")

def main():
    spark = create_spark_session()
    input_data = "/home/jun3/src/data/"
    output_data = "/home/jun3/out/"
    
    #for AWS service
    #input_data = "s3a://jun-capstone/"
    #output_data = "s3a://jun-capstone/out/"
    
    i94port_table(input_data, output_data)
    
    airport_i94_join(spark, input_data, output_data)
    airport(spark, output_data)
    df_i94port2(spark, output_data)
    
    us_cities_temperature_join_table(spark, input_data, output_data)
    us_cities_from_temp_cities_join_table(spark, input_data, output_data)
    us_cities_monthly_temperatures(spark, input_data, output_data)
    
    immigration(spark, input_data, output_data, "2016-04-01") # manually input execution_date instead of from airflow
    

if __name__ == "__main__":
    main()
