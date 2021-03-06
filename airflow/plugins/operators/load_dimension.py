from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 insert_sql='',
                 mode='truncate',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'TRUNCATE TABLE {self.table};')
            self.log.info("Deleting complete.")

        sql = """
            INSERT INTO {table}
            {insert_sql};
        """.format(table=self.table, select_sql=self.insert_sql)

        self.log.info(f'Loading data into {self.table} dimension table...')
        redshift_hook.run(sql)
        self.log.info("Loading complete.")

