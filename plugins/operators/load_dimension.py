from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 truncate_and_reload=False,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_and_reload = truncate_and_reload

    def execute(self, context):
        self.log.info(f"Loading dimension table {self.table} into Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate table if needed
        if self.truncate_and_reload:
            self.log.info(f"Truncating table {self.table} before loading")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        # Generate and log SQL
        insert_query = f"INSERT INTO {self.table} {self.sql_query}"
        self.log.info(f"Running query: {insert_query}")

        # Run query to load table
        redshift.run(insert_query)
        self.log.info(f"Dimension table {self.table} loaded successfully.")
