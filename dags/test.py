import pandas as pd
from io import StringIO
import re 
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
import json

url='https://drive.google.com/file/d/126Q6_GgrcEQDxFBkEEdhiA0UA2BgSK6G/view'
file_id=url.split('/')[-2]
download_data='https://drive.google.com/uc?id=' + file_id
df = pd.read_json(download_data)
print(df)
df['blockchainEvent'] = df['blockchainEvent'].apply(json.dumps)

conn_string = 'postgresql://airflow:airflow@127.0.0.1:54320/datalake'
engine = create_engine(conn_string, echo=True)

conn=engine.raw_connection()

df.to_sql('cryptopunks', engine,schema='ods',index=False,if_exists='append',dtype={'blockTimestamp': sqlalchemy.types.VARCHAR(), 
                   'contractAddress': sqlalchemy.types.VARCHAR(),
                   'tokenId': sqlalchemy.types.VARCHAR(),
                   'fromAddress':sqlalchemy.types.VARCHAR(),
                   'toAddress':sqlalchemy.types.VARCHAR(),
                   'quantity': sqlalchemy.types.VARCHAR(),
                   'blockchainEvent': sqlalchemy.types.VARCHAR(),
                   'transactionType': sqlalchemy.types.VARCHAR()})


#load to staging

cur=conn.cursor()
cur.execute("""
insert into staging.cryptopunks
select cast("blockTimestamp" as timestamp) blockTimestamp,
"contractAddress",
cast("tokenId" as bigint) tokenId,
"fromAddress",
"toAddress",
cast("quantity" as bigint) quantity,
"blockchainEvent",
"transactionType" 
from  ods.cryptopunks source
where not exists (
select * from staging.cryptopunks target
where target.blocktimestamp =cast(source."blockTimestamp" as timestamp) 
and target.contractAddress =source."contractAddress"
and target.tokenId =cast(source."tokenId" as bigint)
and target.fromAddress =source."fromAddress"
and target.toAddress =source."toAddress"
)
""")

conn.commit()
#conn.commit()