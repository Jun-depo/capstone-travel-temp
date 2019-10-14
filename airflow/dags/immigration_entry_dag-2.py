from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.operators import SparkDataQualityOperator, CsvDataQualityOperator
import os

from airflow.macros import ds_format
from datetime import datetime, timedelta
import logging

input_data = "/home/jun3/src/data/"
output_data = "/home/jun3/src/out/"


# os.getcwd() = "/home/jun3" -- home directory in this case
base_dir = os.getcwd() + '/src/'

dag = DAG(
    'immgration_entry1_dag',
    start_date=datetime(2016, 1, 1, 0, 0, 0, 0),
    end_date=datetime(2016, 12, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    max_active_runs=1
)

## execution_date = {{ ds }}
immigration_table_task = BashOperator(
    task_id='make_immigration_table',
    bash_command="python " + base_dir + 'immigration.py' + " " + input_data + " " + output_data  + " {{ ds }}",
    retries=2,
    dag=dag)    


i94port_task = BashOperator(
    task_id='make_i94port_table',
    bash_command="python "  + base_dir + 'i94port.py' + " "+ input_data + " " + output_data,
    retries=2,
    dag=dag)

airport_i94port_task = BashOperator(
    task_id='make_airport_i94port_staging_table',
    bash_command="python "  + base_dir + 'airport_i94_join.py' + " "+ input_data + " " + output_data,
    retries=2,
    dag=dag)

airport_task = BashOperator(
    task_id='make_airport_table',
    bash_command="python "  + base_dir + 'airport.py' + " " + output_data,
    retries=2,
    dag=dag)

i94port2_task = BashOperator(
    task_id='make_i94port2_airport_table',
    bash_command="python "  + base_dir + 'i94port2_airport.py' + " " + output_data,
    retries=2,
    dag=dag)

temperature_cities_join_task = BashOperator(
    task_id='make_temperature_cities_join_table',
    bash_command="python "  + base_dir + 'temperature_cities_join.py' + " "+ input_data+ " " + output_data,
    retries=2,
    dag=dag)

us_cities_task = BashOperator(
    task_id='make_us_cities_table',
    bash_command="python "  + base_dir + 'us_cities.py' + " "+ input_data  + " " + output_data,
    retries=2,
    dag=dag)

us_cities_temperature_task = BashOperator(
    task_id='make_us_cities_temperature_table',
    bash_command="python "  + base_dir + 'us_city_recent_ave_monthly_temperature.py' + " "+ input_data + " "+ output_data,
    retries=2,
    dag=dag)


# all csv files are in file format
csv_tables = [f for f in os.listdir(output_data) if os.path.isfile(os.path.join(output_data, f))]
csv_data_checks = CsvDataQualityOperator(
    task_id='csv_data_quality_checks',
    tables = csv_tables,
    output_data_dir = output_data,
    retries=2,
    dag=dag
)

# for check files of current run
base_tables = ['us_city_temperatures_us_cities_join_table.parquet', 
 'USA_cities_from_temp.parquet',
 'i94port2.parquet',
 'airport_i94port_join_table_cleaned.parquet',
 'USA_city_recent_average_monthly_temperatures.parquet',
 'airport_table.parquet']

# exe_month_yr = datetime.strptime('{{ ds }}', "%Y-%m-%d").strftime("%b%y").lower() 
# parquet_data_tables = 

parquet_data_checks = SparkDataQualityOperator(
    task_id='parquet_data_quality_checks',
    execution_date = "{{ ds }}",
    tables = base_tables,
    output_data_dir = output_data,
    retries=2,
    dag=dag
)

analy_tables_out_dir = "/home/jun3/src/temperature_entry_Anal/"
analtical_table_task = BashOperator(
    task_id='make_temperature_entry_analtical_table',
    bash_command="python "  + base_dir + 'analytical_table.py' + " "+ output_data+ " " + analy_tables_out_dir + " {{ ds }}",
    retries=2,
    dag=dag)


anal_tables = [f for f in os.listdir(analy_tables_out_dir) if os.path.isfile(os.path.join(analy_tables_out_dir, f))]
anal_data_checks = CsvDataQualityOperator(
    task_id='anal_data_quality_checks',
    tables = anal_tables,
    output_data_dir = analy_tables_out_dir,
    retries=2,
    dag=dag
)


# Put the tasks in the right order

temperature_cities_join_task >> us_cities_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks

temperature_cities_join_task >> us_cities_temperature_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks

temperature_cities_join_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks

i94port_task >> airport_i94port_task >> airport_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks

i94port_task >> airport_i94port_task >> i94port2_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks

i94port_task >> csv_data_checks >> analtical_table_task >> anal_data_checks

immigration_table_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks

airport_task >> parquet_data_checks >> analtical_table_task >> anal_data_checks
