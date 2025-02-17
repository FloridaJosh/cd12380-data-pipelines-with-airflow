from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id,
                 tests=None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        self.log.info("Starting data quality checks")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Initialize counter variable and list for failing tests
        error_count = 0
        failing_tests = []

        # Iterate through each test passed into the operator
        for test in self.tests:
            # Get the SQL and expected result for that SQL
            sql = test.get("sql")
            expected = test.get("expected_result")

            # Log the SQL statement, run it, and compare to expected results
            self.log.info(f"Running data quality test: {sql}")
            records = redshift.get_records(sql)
            if len(records) < 1 or records[0][0] != expected:
                error_count += 1
                failing_tests.append(sql)
                self.log.error(f"Data quality check failed for query: {sql}")

        # If the error count is 1 or more, log an error message and throw an exception
        if error_count > 0:
            self.log.error(f"One ore more data quality check(s) failed. Failing tests: {failing_tests}")
            raise AirflowException("One or more tests failed!")
        
        # If we get to this point in the code, there were no problems so log a success message
        self.log.info("All data quality checks passed")