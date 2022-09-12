import pandas as pd
from io import StringIO
import re 
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
import json

load_gdrive={
    "url":"https://drive.google.com/file/d/1prET30LkR4GS3skOkhJn3DFy1w1NNw0S/view",
    "ods_tb_name":"metadata"
}

json_url=load_gdrive["url"]
print(json_url)

url='https://drive.google.com/file/d/1prET30LkR4GS3skOkhJn3DFy1w1NNw0S/view'
file_id=url.split('/')[-2]
download_data='https://drive.google.com/uc?id=' + file_id
df = pd.read_json(download_data)
print(df.head())
df['properties'] = df['properties'].apply(json.dumps)

conn_string = 'postgresql://airflow:airflow@127.0.0.1:5432/datalake'
engine = create_engine(conn_string, echo=True)

conn=engine.raw_connection()

"""
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
"""


#conn.commit()