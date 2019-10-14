from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.python_operator import PythonOperator

class StageToRedshiftOperator(BaseOperator):
    """The operator is for staging tables from data in json format to Redshift.
    It requires the following inputs:
    redshift_conn_id: to make connection to Redshift, configured at Airflow GUI under Admin/connection
    table:  name of staging table
    s3_bucket: name of s3 bucket 
    s3_key: sub-directory under the s3_bucket where data is located
    aws_credentials_id: contains aws_access_key_id and aws_secret_access_key. Needs to be configured at Airflow GUI under Admin/connection        
    json: provides information on how json file will be interpreted.      
    """
    ui_color = '#358140'
    
    template_fields = ("s3_key",) ### contains info that needs to be interpreted
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'    
        {};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",                
                 table="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",        
                 json = "",              
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id        
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json = json
        
       
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 bucket to Redshift")
        
        rendered_key = self.s3_key.format(**context) ### to be interpreted based on context
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
               
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json         
        )
        redshift.run(formatted_sql)



