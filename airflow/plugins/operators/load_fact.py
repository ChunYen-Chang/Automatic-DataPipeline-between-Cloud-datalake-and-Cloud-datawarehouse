# import necessary packages 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
        Description: 
            This class helps us to access to AWS redshift, get data from songs and eventsstaging tables, 
            and form a Fact table in AWS Redshift
    """
    # define the color of LoadFactOperator icon in Airflow webUI
    ui_color = '#F98866'

    # define parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 create_table_sql,
                 insert_table_sql,
                 mode,              
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_table_sql = insert_table_sql
        self.mode = mode


    def execute(self, context):
        """
            Description: 
                The main purpose of this function is getting data from staging tables and forming fact table. It has several steps. 
                Step1: access to AWS redshift
                Step2: create songplays table (Fact table)
                Step3: insert data into songplays table
        """
        # access to AWS redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # create songplays table (Fact table)
        self.log.info('Create fact table {} in Redshift'.format(self.table))
        redshift.run(self.create_table_sql)
        
        # insert data into songplays table: 
        #       it has two mode. If self.mode is equal to append, it will put the new data rows below the existing 
        #       data rows. If the self.mode is equal to overwrite, it will delete all existing data row and put new 
        #       data rows in the songplay table.
        self.log.info('Insert data into fact table {}'.format(self.table))
        if self.mode == 'append':
            insert_sql = f"INSERT INTO {self.table} {self.insert_table_sql}"
        elif self.mode == 'overwrite':
            insert_sql = f"DELETE FROM {self.table}; INSERT INTO {self.table} {self.insert_table_sql}"
        redshift.run(insert_sql)