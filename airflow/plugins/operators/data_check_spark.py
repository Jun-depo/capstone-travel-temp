from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

class SparkDataQualityOperator(BaseOperator):
    
    """ For checking data quality after loading tables. In this case, we counts row records in the table >= 1.
    tables: a list of tables that will be checked by DataQualityOperator          
    """
    ui_color = '#89DA59'
    template_fields = ('execution_date',)
    
    @apply_defaults
    def __init__(self,
                 #redshift_conn_id = "redshift",
                 execution_date,
                 tables = [],
                 output_data_dir = "",                 
                 *args, **kwargs):

        super(SparkDataQualityOperator, self).__init__(*args, **kwargs)        
        self.tables = tables
        self.output_data_dir = output_data_dir
        self.execution_date = execution_date
        
    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet'
        
        ### loop through all tables for the quality check
        month_year = datetime.strptime(self.execution_date, '%Y-%m-%d').strftime('%b%y').lower()
        tables = self.tables + [f'immigration_table_{month_year}.parquet']
        for table in tables:
            table = self.output_data_dir + table
            
            if table.split(".")[1] == "parquet":
                df = pq.read_table(table) 
                num_records = df.num_rows
                if num_records < 1 or None:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                logging.info(f"Data quality on table {table} check passed with {num_records} records")

