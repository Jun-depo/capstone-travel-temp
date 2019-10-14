from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

import pandas as pd
import pyarrow.parquet as pq

class CsvDataQualityOperator(BaseOperator):
    
    """ For checking data quality after loading tables. In this case, we counts row records in the table >= 1.
    tables: a list of tables that will be checked by DataQualityOperator          
    """
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 tables = [],
                 output_data_dir = "",                 
                 *args, **kwargs):

        super(CsvDataQualityOperator, self).__init__(*args, **kwargs)        
        self.tables = tables
        self.output_data_dir = output_data_dir
        
    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet'
        
        ### loop through all tables for the quality check
        for table in self.tables:
            table = self.output_data_dir + table
            if table.split(".")[1] == "csv":
                df = pd.read_csv(table)
                num_records = len(df)
                if num_records < 1 or None:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                logging.info(f"Data quality on table {table} check passed with {num_records} records")
            
