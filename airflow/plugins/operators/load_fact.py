from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table = '',
                 insert_sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = insert_sql

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        self.log.info(f'Loading data into {self.table} fact table...')

        sql = """
            INSERT INTO {table}
            {insert_sql};
        """.format(table=self.table, select_sql=self.insert_sql)

        redshift_hook.run(sql)
        self.log.info("Loading complete.")

