from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    truncate_sql_format = """
    CREATE TABLE {destination_table} IF NOT EXISTS
    TRUNCATE TABLE {destination_table}
    """

    insert_sql_format = """
    INSER INTO {destination_table} {sql_query}
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 destination_table="",
                 append_only=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.destination_table=destination_table
        self.append_only=append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Starting attempt to load data to dimension table")
        if self.append_only == False:
            facts_sql = LoadDimensionOperator.truncate_sql_format.format(
                destination_table = self.destination_table
            )
            redshift.run(facts_sql)
        LoadDimensionOperator.insert_sql_format.format(
            destination_table = self.destination_table,
            sql_query = self.sql_query
        )
        redshift.run(facts_sql)
        self.log.info('LoadDimensionOperator not implemented yet')
