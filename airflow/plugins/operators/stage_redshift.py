# import necessary packages 
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
        Description: 
            This class helps us to access to S3 bucket and AWS redshift, get data from this S3 bucket, 
            and save the data into Redshift.
    """
    # define the color of StageToRedshiftOperator icon in Airflow webUI
    ui_color    = '#358140'

    # define the sql command of copying **song** files from S3 to AWS redshift
    copy_song_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        BLANKSASNULL 
        EMPTYASNULL
        FORMAT AS PARQUET
    """
    
    # define the sql command of copying **event** files from S3 to AWS redshift
    copy_event_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        BLANKSASNULL
        EMPTYASNULL
        FORMAT AS PARQUET
    """
 
    # define parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 create_table_sql,
                 s3_bucket,
                 s3_key,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        
    def execute(self, context):
        """
            Description: 
                The main purpose of this function is copying data from a S3 bucket to AWS Redshift. It has several steps. 
                Step1: access to AWS S3 bucket and AWS Redshift
                Step2: drop staging tables in Redshift
                Step3: create staging table in Redshift
                Step4: copy data from S3 to Redshift staging table
        """
        # access to AWS S3 bucket and AWS Redshift
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # drop staging tables in Redshift
        self.log.info('Drop staging table {} in Redshift'.format(self.table))
        redshift.run('DROP TABLE IF EXISTS {}'.format(self.table))
        
        # create staging tables in Redshift       
        self.log.info('Create table_{} in Redshift'.format(self.table))
        redshift.run(self.create_table_sql)
           
        # copy data from S3 to Redshift staging tables
        self.log.info("Copy {} data from S3 to Redshift staging table".format(self.table))
        s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        self.log.info(s3_path)
        
        if self.table == "staging_songs":
            copy_sql = StageToRedshiftOperator.copy_song_sql.format(self.table,
                                            s3_path,
                                            credentials.access_key,
                                            credentials.secret_key,
                                            )
        elif self.table == "staging_events":
            copy_sql = StageToRedshiftOperator.copy_event_sql.format(self.table,
                                              s3_path,
                                              credentials.access_key,
                                              credentials.secret_key,
                                             )
        redshift.run(copy_sql)
