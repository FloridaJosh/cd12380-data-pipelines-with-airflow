from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator

class CreateTablesOperator(BaseOperator):
    """
    Custom Airflow operator to execute a SQL script in Redshift.
    """

    def __init__(self, redshift_conn_id, sql_file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_file_path = sql_file_path

    def execute(self, context):
        self.log.info(f"Reading SQL script from {self.sql_file_path}")

        # Read the SQL file
        with open(self.sql_file_path, 'r') as file:
            sql_commands = file.read()

        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Executing SQL script...")
        redshift.run(sql_commands)
        self.log.info("Tables created successfully in Redshift.")
