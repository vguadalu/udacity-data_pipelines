from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_check={},
                 expected_nulls={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_check = table_check
        self.expected_nulls = expected_nulls

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table,column in self.table_check.items():
            self.log.info(f'Starting data quality check for {table} table')
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table};')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality failed for {table} table')
                self.log.error(f'Data quality failed for {table} table')
            
            self.log.info(f'Checking {table} for nulls in {column}')
            check_null = f"SELECT COUNT(*) FROM {table} WHERE {column} is null"
            records = redshift.get_records(check_null)
            if records[0][0] != self.expected_nulls[column]:
                raise ValueError(f"Data quality check failed. {table} contains null in {column} column")
            self.log.info(f'Data quality passed for {table} table. Success!')
