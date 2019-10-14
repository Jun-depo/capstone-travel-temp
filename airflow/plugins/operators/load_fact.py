from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ To load fact table (eg. songplays table). It required the following inputs.
    table: name of the table.
    redshift_conn_id: to make connection to Redshift, configured at Airflow GUI under Admin/connection.
    insert_fact_sql: sql statement for data insertion into the fact table
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "redshift",
                 insert_fact_sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_fact_sql = insert_fact_sql
            
    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"inserting data into Redshift fact {self.table} table")
        redshift.run(self.insert_fact_sql)
