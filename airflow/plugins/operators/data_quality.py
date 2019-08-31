# import necessary packages 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
        Description: 
            This class helps us to check data quality on each dimensional table. It will check whether each dimensional
            table contains data or not. For example, if a dimensional table contains no data, it will be categorized into
            error category and raise an error message on the log.
    """
    # define the color of LoadDimensionOperator icon in Airflow webUI
    ui_color = '#89DA59'

    # define parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

        
    def execute(self, context):
        """
            Description: 
                The main purpose of this function is checking data quality on each dimensional table. It has two steps. 
                Step1: access to AWS redshift
                Step2: data quality check (by counting the number of row on each dimensional table)
        """
        # access to AWS redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # data quality check (by counting the number of row on each dimensional table)
        self.log.info('{}-Data Quality check...'.format(self.table))
        record = redshift.get_records('SELECT COUNT(*) FROM {}'.format(self.table))
        if len(record) < 1 or len(record[0]) < 1:
            raise ValueError('Data quality check failed. {} returned no results'.format(self.table))
            
        num_record = record[0][0]
        if num_record < 1:
            raise ValueError('Data quality check failed. {} contained 0 rows'.format(self.table))
        
        self.log.info('{}-Data Quality check passed.'.format(self.table))