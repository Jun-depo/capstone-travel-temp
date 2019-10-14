# Building Data Pipeline with Spark and Airflow to Study the Relationship Between Temperature and Traveling into US Ports.

## Data Engineering Capstone Project


## Project Summary:


I94 immigration data from US government contain travel entry data into United States from other countries including their entry_date, departure_date, visa_categrory, visa_type etc. How do we corrlate the travel entry data with other data to understand what factors can affect traveling into USA?  I used Spark and Pandas to create data processing pipeline to ETL data for several tables. I used Airflow to mange the workflow and task dependencies.  


The data pipelie produce finally aggregated data into a set of analytic tables that can be used for studying the relationship between international traveling and city temperatures in USA. The data was used for plotting to visualization of the relationship between temperature and traveling counts in top 20 cities in USA. The picture below shows plots for 9 cities.  

<img src="temp_travel_polts.png" style="width:900px;height:620px;">


In most cities except warm weather cities, traveling increases almost in linear fashion with the increase of the temperature.  Warm weather cities are exceptions. Since the weather was never really cold in warm cities, traveling becomes also less prohibitive in these cities, resulting less traveling reduction in winter season. 


## Step 1: Scope the Project and Gather Data

### Data Sources/Dictionary:
In this project, we have data from the following sources:
* airport (**airport-codes_csv.csv**)
* city demography (**us-cities-demographics.csv**)
* temperature (**Global_city_temperatures.csv**)
* additional label information (**I94_SAS_Labels_Descriptions.SAS**). **"i94port.txt"** and **i94state_code.txt** were obtainted from I94_SAS_Labels_Descriptions.SAS file. 
* external data: **"uscities_simplemap.csv"** from (https://simplemaps.com/data/us-cities). I used the external data, due to incorrect longitude and latitude data in Global_city_temperatures.csv file, that map New York City to New Jersey and Chicago to Wisconsin. Geo-data was used to select the right cities after joining the data with the same city names (use < 180 km  as the filter). 

* **i94 immigration tables**: Since I can not download immigration table files directly from Udacity workspace. I open these files in jupyter notebook at the workspace as suggested inside the notebook and save them as parquet files and download to my local machine. I kept the original file names for the parquet folder names, but under data folder such as ("/home/jun3/src/data/i94_jan16_sub.parquet/"). I have all 12 month data under data folder. Airflow schedules runs with should follow the same scheme as the origianl data. 

### Technical Tools


* The data need to be extracted, cleaned and transformed before the data analyses. I choose to use **Spark**, **Pandas** and **Airflow** as main technical tools for the project. 
* **Spark** is good for:
* processing large datasets in both distributed and parallel fashion.
* Spark/Hadoop can run on low cost commodity hardwares.
* Spark can create table schema on the fly, very convenient to create pipelines for different analytical purposes  
* Spark dataframe contains many tools for cleaning/processing and some tools for basic machine learning. 
* Spark allows output data to be saved in parquet format that is columnar and compressed. Both of these characters speed up query process. File comprssion also saves storage and query cost if queried through cloud based service such as Amazon Athena. 


* **Pandas** is good tool for smaller datasets:
* Very flexible for cleaning the data.
* Good for plotting with external libaries such as **Matplotlib** and **Seaborn**. 


* **Airflow** is a great tool for managing ETL processes:
* It manages the job/task flow and dependencies, make sure certain tasks start first and finish before starting other processes.
* It schedules data processing with execution_date. Timestamps of the Airflow scheduling allow only certain data fitting with certain date requirement that can be processed one at the time, that effectively partitions large data into smaller amounts, avoiding large amount of data overwhelming the system. 
* Airflow Scheduling backfills runs of previous timestamp.
* It has web based visualization tools making the process very clear, giving color coded warning if something fails. It also contains logs for debugging and SLA for managing service deadline. 
* It has tools to create connections to cloud based services (such as AWS services).  


### Step 2: Explore and Assess the Data
#### Data cleaning and processing

I also include **immg_table_test_Apr.ipynb** [here](src/immg_table_test_Apr.ipynb)that provides an example of data cleaning and primary exploratory data analysis for immigration table, and merging with airport_i94port_join_table for primary traveling analysis.  I checked numbers of null value in each columns through counting_null_number_inColumns() function in helper.py. 

* Data cleaning and processing is rather tedious. The general process involves:
* (1) Get rid unrelated columns. df.drop(column_names)
* (2) Drop or fill null values.  df.dropna()
* (3) Get ride of duplicated data.  Use df.dropDuplicates() in Spark. df.drop_duplicates() in pandas
* (3) Extract and create new columns from existing data if needed.


* I documented the detail data cleaning process as comments inside submiitted python scripts.  


* The ETL process need to clean and preserve most of the original data. The analysis wouldn't be accurate if too much data get deleted.  In this cases, I preserved at about high 80+% comparing to original monthly i94_immigration table data.   


### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

I intend to create Data Lake with Spark due to flexibility to create specific schemas for specific analytic purpose.  The processed can be relatively easy to change for a different analytic purpose. Spark is compatible with ingesting large amount of data into dataframe, can process data in distributed and parallel fashion that is fast and effiecient.  It also provides data cleaning tools.

Airflow is used to manage data pipeline to orderly create multiple tables that is consistent with data pipeline depedencies. 

#### 3.2 Mapping Out Data Pipelines
1. Create initial tables
2. Create join tables 
3. Use join tables to create final sets of tables
4. Some tables need to be created earlier in order to create next tables. This is managed by Airflow through orders of task in immigration_entry_dag-2.py file. 
5. Finally, data quality was checked before making analytical tables.
6. Make analytical tables onlt after other tables have passed data quality checks.  In that way, the analytical tables are ready for final data analysis. 
7. The data analysis and plotting was performed by using Pandas, matplotlib and seaborn in **temperature_port_entry_analysis.ipynb** file. 

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
The following python scripts was used to build the data pipeline to create the data model.

#### 4.1a Scripts
* **'i94port.py'**[link](src/i94port.py):  create i94port_pd.csv table from "i94port.txt"
* **'airport_i94_join.py'**[link](src/airport_i94_join.py): create airport_i94_join table by joining i94port_pd.csv table and a table from airport-codes_csv.csv.

* **'airport.py'**[link](src/airport.py): create airport table from airport_i94port_join_table 
* **'i94port2_airport.py'**[link](src/i94port2_airport.py): create i94port2 table from airport_i94port_join_table 

* **'temperature_cities_join.py'**[link](src/temperature_cities_join.py):  creature temperature_cities_join table from "Global_city_temperatures.csv" and "uscities_simplemap.csv"
* **'us_cities.py'** [link](src/us_cities.py): create us_cities table from temperature_cities_join table
* **'us_city_recent_ave_monthly_temperature.py'**[link](src/us_city_recent_ave_monthly_temperature.py):  create us_city_recent_ave_monthly_temperature table from temperature_cities_join table. 

* **'immigration.py'** [link](src/immigration.py):  i94_immigration table was created month with monthly timestamps. Airflow scheduler schedules the run in monthly fashing matching the names of i94_immigration data such as "i94_apr16_sub" through "i94_{month_year}_sub" can be extracted execution_date

* **'analytical_table.py'** [link](src/analytical_table.py): The script is used to create analytical table. 
* **'helper.py'**[link](src/helper.py): helper functions for data cleaning and processing during ETL


* **etl_without_airflow.py**[link](src/etl_without_airflow.py): To run the script without airflow. Manually provide execution_date for immigration table as exe_date ="2016-04-01" 

* **temperature_port_entry_analysis.ipynb**[link](src/temperature_port_entry_analysis.ipynb).  The script uses the analytical tables to conduct data analysis and create visualization plots.

#### Running without Airflow
* **etl_without_airflow.py**[link](src/etl_without_airflow.py): need to change the directory to where the script is located and helper.py is located in the same directory. Otherwise, helper functions are not imported properly into the script.   


#### 4.1b Run the pipelines with Airflow
* The scripts in airflow folder that include **immigration_entry_dag-2.py** (home/jun3/airflow/dags/immigration_entry_dag-2.py) at my local machine, (home/workspace/airflow/dags/immigration_entry_dag-2.py) at Udacity workspace.  The DAG runs mostly with BashOperator that avoids writing long python functions inside DAG python script.  Airflow need to be configured properly in order to run the script.  In my case, I changed database setting to run on postgresql instead default sql_alchemy as "sql_alchemy_conn = postgresql+psycopg2://postgres:postgres@127.0.0.1/jundb".  Other things also need to be in right folders inside airflow folder. 

The graphs illustrate the DAG run of the pipeline below. 

<img src="dag_run3.png" style="width:1000px;height:300px;">

<img src="dag_run3b.png" style="width:600px;height:300px;">

#### Directory/folder structures
Sometime correct directory locations are important for running scripts.  I provide the information here, in case they are useful for contructing DAG runs. 
* airflow folder locates at home directory (/home/jun3/airflow/)
* src folder (containing scripts and data) locates at home directory (/home/jun3/src/)
* input_data folder locates under src folder (/home/jun3/src/data/)
* output_data folder locates under src folder (/home/jun3/src/out/)
* analytical tables locates at (/home/jun3/src/temperature_entry_anal)
* Below I show some of the items insider /home/jun3/src/data/ since data folder will not be included in the submission. 

<img src="data_folder.png" style="width:240px;height:300px;">



#### 4.2 Data Quality Checks
* **data_check_csv.py** (/home/jun3/airflow/plugins/operators/data_check_csv.py) and **data_check_spark.py** (/home/jun3/airflow/plugins/operators/data_check_spark.py) were used to count numbers of rows in each table. All tables contain good number of data as illustrated below.   

<img src="dag_run3_pq_ck.png" style="width:800px;height:160px;">

* I use pyarrow.parquet to read parquet files and count rows. I couldn't run spark there,  due to running another Spark session inside the data_check operator interferes with running of the DAG pipeline.  pyarrow.parquet runs slower than Spark.  

### 5. Different use case situations:  

1. Immigration data are provided monthly from the government. The current schedule runs are monthly match the time frequency of data immigration tables. However, if the data is much larger. The provided immigration data will be further partitioned by day(even hour) instead of month. In such cases the running schedule will be daily.   


2. If the data was increased by 100x.  The reasonable approach will be to partition data hourly or every 4 hours, then run the DAG on hourly or every 4 hours (0 */4 * * *)* schedule. 


3. If the data populates a dashboard that must be updated on a daily basis by 7am every day, the DAG will be running daily with service contract in airflow with SLA (service level agreement) linked to email accounts to report SLA misses. To prevent SLA misses, the pipeline can be run by a larger and more powerful clusters if needed. 


4. If The database needed to be accessed by 100+ people, the output tables will be loaded to AWS S3 bucket(s).  The data need to be duplicated/multipicated with sufficient copies for the access.  If users locate at different region, data also need to be copied into buckets at different regional zones for fast data access.  

## 6. Temperature and Travel Entry Analysis
* Temperature-travel entry analysis was done with **temperature_port_entry_analysis.ipynb**[link](src/temperature_port_entry_analysis.ipynb).
* The temperature and travel counts were extracted from each monthly analytical table, plotted with regplot() of **seaborn** library.  The result is shown in the summary at the beginning of this report, also in temperature_port_entry_analysis.ipynb.
