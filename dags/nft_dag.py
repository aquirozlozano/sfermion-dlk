import pandas as pd
from io import StringIO
import re 
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.exc import OperationalError
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import json

default_args = {
    "owner": "andyquiroz",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "nft_dag",
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily",
    default_args=default_args,
)


class load_to_ods_operator(BaseOperator):
    #variables
    @apply_defaults
    def __init__(self, 
        jdbc_url,
        url,
        tableName,
        *args,
        **kwargs):
        super().__init__(*args, **kwargs)
        self.jdbc_url=jdbc_url
        self.url=url
        self.tableName=tableName
        
    def read_data(self):

        engine = create_engine(self.jdbc_url, echo=True)
        #reading the data from the url
        try:

            url=self.url
            file_id=url.split('/')[-2]
            download_data='https://drive.google.com/uc?id=' + file_id
            df = pd.read_json(download_data)
            print(df.head())
            if self.tableName =="transactions":
                print("for transactions table")
                df['blockchainEvent'] = df['blockchainEvent'].apply(json.dumps)

                df.to_sql('transactions', engine,schema='ods',index=False,if_exists='append',dtype={'blockTimestamp': sqlalchemy.types.VARCHAR(), 
                   'contractAddress': sqlalchemy.types.VARCHAR(),
                   'tokenId': sqlalchemy.types.VARCHAR(),
                   'fromAddress':sqlalchemy.types.VARCHAR(),
                   'toAddress':sqlalchemy.types.VARCHAR(),
                   'quantity': sqlalchemy.types.VARCHAR(),
                   'blockchainEvent': sqlalchemy.types.VARCHAR(),
                   'transactionType': sqlalchemy.types.VARCHAR()})

            elif self.tableName =="metadata":
                df['properties'] = df['properties'].apply(json.dumps)
                df.to_sql('metadata', engine,schema='ods',index=False,if_exists='append',dtype={'contract_address': sqlalchemy.types.VARCHAR(), 
                   'token_id': sqlalchemy.types.VARCHAR(),
                   'name': sqlalchemy.types.VARCHAR(),
                   'description':sqlalchemy.types.VARCHAR(),
                   'minted_timestamp':sqlalchemy.types.VARCHAR(),
                   'supply': sqlalchemy.types.VARCHAR(),
                   'image_url': sqlalchemy.types.VARCHAR(),
                   'media_url': sqlalchemy.types.VARCHAR(),
                   'external_url': sqlalchemy.types.VARCHAR(),
                   'properties': sqlalchemy.types.VARCHAR(),
                   'metadata_url': sqlalchemy.types.VARCHAR(),
                   'last_refreshed': sqlalchemy.types.VARCHAR()})

        except (NameError,TypeError,UnboundLocalError,ValueError) as error:
            print('Declare la variable correcta:',error) 
            raise
    
    def execute(self,context):
        self.read_data()

class load_to_staging_operator(BaseOperator):
    @apply_defaults
    def __init__(self, 
        jdbc_url,
        tableName,
        *args,
        **kwargs):
        super().__init__(*args, **kwargs)
        self.jdbc_url=jdbc_url
        self.tableName=tableName

    def load(self):

        try:

            engine = create_engine(self.jdbc_url, echo=True)

            conn=engine.raw_connection()

            if self.tableName=="transactions":
                
                cur=conn.cursor()
                cur.execute("""
                insert into staging.transactions
                select cast("blockTimestamp" as timestamp) blockTimestamp,
                "contractAddress",
                cast("tokenId" as bigint) tokenId,
                "fromAddress",
                "toAddress",
                cast("quantity" as bigint) quantity,
                "blockchainEvent",
                "transactionType" 
                from  ods.transactions source
                where not exists (
                select * from staging.transactions target
                where target.blocktimestamp =cast(source."blockTimestamp" as timestamp) 
                and target.contractAddress =source."contractAddress"
                and target.tokenId =cast(source."tokenId" as bigint)
                and target.fromAddress =source."fromAddress"
                and target.toAddress =source."toAddress"
                )
                """)

            elif self.tableName=="metadata":
                cur=conn.cursor()
                cur.execute("""
                insert into staging.metadata
                select "contract_address",
                cast("token_id" as bigint) token_id,
                "name",
                "description",
                cast("minted_timestamp" as timestamp) minted_timestamp,
                cast("supply" as integer) supply,
                "image_url",
                "media_url",
                "external_url",
                "properties",
                "metadata_url",
                cast("last_refreshed" as timestamp) last_refreshed
                from ods.metadata source
                where not exists (
                select * from staging.metadata target
                where target.token_id=cast(source."token_id" as bigint)
                )
                """)
            
            conn.commit()

        except (NameError,TypeError,UnboundLocalError,ValueError) as error:
            print('Declare la variable correcta:',error) 
            raise

    def execute(self,context):
        self.load()

with dag:

    begin = DummyOperator(task_id="begin")

    conn_string = 'postgresql://airflow:airflow@postgres:5432/datalake'

    load_gdrive=[
        {
        "url":"https://drive.google.com/file/d/126Q6_GgrcEQDxFBkEEdhiA0UA2BgSK6G/view",
        "ods_tb_name":"transactions"
    },
    {
        "url":"https://drive.google.com/file/d/1prET30LkR4GS3skOkhJn3DFy1w1NNw0S/view",
        "ods_tb_name":"metadata"
    }]

    for row in load_gdrive:
        url=row["url"]
        tableName=row["ods_tb_name"]
        load_to_ods = load_to_ods_operator(
        task_id=f'load_to_ods_{tableName}',
        jdbc_url=conn_string,
        url=url,
        tableName=tableName
        )

        load_to_staging = load_to_staging_operator(
        task_id=f'load_to_staging_{tableName}',
        jdbc_url=conn_string,
        tableName=tableName
        )

        begin >> load_to_ods >> load_to_staging