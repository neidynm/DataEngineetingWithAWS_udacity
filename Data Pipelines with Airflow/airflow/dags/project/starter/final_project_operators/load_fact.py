from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    facts_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT
        {groupby_column},
        MAX({fact_column}) AS max_{fact_column},
        MIN({fact_column}) AS min_{fact_column},
        AVG({fact_column}) AS average_{fact_column}
    FROM {origin_table}
    GROUP BY {groupby_column};
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 fact_column="",
                 groupby_column="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.origin_table=origin_table
        self.destination_table=destination_table
        self.fact_column=fact_column
        self.groupby_column=groupby_column

    def execute(self, context):
        self.log.info('Staring loading fact data')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql = LoadFactOperator.facts_sql_template.format(
            destination_table = self.destination_table,
            groupby_column = self.groupby_column,
            fact_column= self.fact_column,
            origin_table = self.origin_table
        )
        redshift.run(facts_sql)
        self.log.info('Finishing loading fact data ')
