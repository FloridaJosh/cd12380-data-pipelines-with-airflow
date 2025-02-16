from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        
    def execute(self, context):
        self.log.info(f"Loading fact table {self.table} into Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Generate and log SQL
        insert_query = f"INSERT INTO {self.table} {self.sql_query}"
        self.log.info(f"Running query: {insert_query}")

        # Run insert query
        redshift.run(insert_query)
        self.log.info(f"Fact table {self.table} loaded successfully.")