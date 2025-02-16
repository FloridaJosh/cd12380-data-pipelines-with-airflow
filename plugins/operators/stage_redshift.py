from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info(f"Staging data from S3 to Redshift into {self.table}")

        # Get the credentials from the Airflow connection
        aws_conn = BaseHook.get_connection(self.aws_credentials_id)
        access_key = aws_conn.login
        secret_key = aws_conn.password

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Render the S3 key to handle dates
        rendered_key = self.s3_key.format(**context)
        self.log.info(f"Rendered S3 Key: {rendered_key}")

        # Create the COPY command using AWS credentials
        copy_query = f"""
            COPY {self.table}
            FROM 's3://{self.s3_bucket}/{rendered_key}'
            ACCESS_KEY_ID '{access_key}'
            SECRET_ACCESS_KEY '{secret_key}'
            JSON '{self.json_path}'
            REGION 'us-west-2'
        """

        self.log.info("Executing COPY command...")
        redshift.run(copy_query)
        self.log.info(copy_query)
        self.log.info("Data staged successfully")
