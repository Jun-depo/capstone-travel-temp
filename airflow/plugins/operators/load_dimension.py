from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ To load dimension table (eg. songplays table). It required the following inputs.
    table: name of the table.
    redshift_conn_id: to make connection to Redshift, configured at Airflow GUI under Admin/connection.
    insert_dim_sql: sql statement for inserting data into a dim table.
    """
    # template_fields = ("", "insert_table_sql",)
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = "",
                 insert_dim_sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table 
        self.insert_dim_sql = insert_dim_sql
                       
    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"inserting data into Redshift dimension {self.table} table")
        redshift.run(self.insert_dim_sql)