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
from pyspark.sql.types import BooleanType
import pandas as pd
import re

from helper import remove_last2_from_colname, change_data_type
import sys

def create_spark_session():
    """ Create a SparkSession to use Spark"""
    conf = SparkConf().set('spark.driver.memory', '6G') \
         .set('spark.executor.memory', "4G")
    #conf = SparkConf()
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

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
    imm_date = datetime.strptime(exe_date, '%Y-%m-%d').strftime('%b%y').lower()
    
    # Open i94 immigration table with correct month_year
    df_imm=spark.read.parquet(input_data + f'i94_{imm_date}_sub.parquet/') 
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
    df_imm3.write.parquet(output_data + f"immigration_table_{imm_date}.parquet", mode ="overwrite", 
        partitionBy=['entry_month', "i94port_city_code", "destination_state_code"])    
    print(f"Write_immigration_table_{imm_date}.parquet")


if __name__ == "__main__":
    # Getting input_data, output_data from Bash commmand inside airflow BashOperator.
    input_data = sys.argv[1]
    output_data = sys.argv[2]  
    
    # get execution data from from Bash commmand inside airflow BashOperator.
    exe_date = sys.argv[3]
    
    #exe_date = "2016-04-01"  ### Just for testing before inserting execution_date from airflow.
    
    spark = create_spark_session()
    immigration(spark, input_data, output_data, exe_date)
    
