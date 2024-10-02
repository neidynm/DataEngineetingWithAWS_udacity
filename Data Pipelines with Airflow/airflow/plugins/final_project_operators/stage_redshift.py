from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key")
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                table="",
                s3_bucket="",
                redshift_conn_id="",
                aws_credentials_id="",
                s3_key="",
                json_file_name="",
                *args,**kwargs):

        super(StageToRedshiftOperator, self).__init__(*args,**kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_file_name=json_file_name
        self.aws_credentials_id=aws_credentials_id

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from S3 bucket {self.s3_bucket} to Redshift table {self.table}.")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Starting attempt to copy data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_key)
        

        if self.json_file_name == "":
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                "dev.public."+ self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                "auto"
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                "dev.public."+ self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                "s3://{}/{}".format(self.s3_bucket, self.json_file_name)
            )
        redshift.run(formatted_sql)
        self.log.info(f"Copied data from {self.s3_bucket} to {self.table} ")
