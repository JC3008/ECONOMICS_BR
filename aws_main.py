from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sqlalchemy
from html.parser import HTMLParser
import psycopg2
import pandas as pd
import boto3 
from datetime  import datetime as dt
import io

time = dt.date(dt.now())


def boto3_aws_pg():
    s3_client = boto3.client('s3')
    s3_client.upload_file("/home/jc/projeto_b3_linux/3_gold/AWS_b3.csv","datalake-josecarlos-977711486277",f"raw-data/fato_b3_{time}.csv",)



# def load_aws_pg():
#     conn_string = 'postgresql://knppost_b3gres:?Wli17j674@sala443-projeto-b3.ckp5huelvu5f.us-east-1.rds.amazonaws.com/dw_b3'  
#     db = create_engine(conn_string)
#     conn = db.connect()
#     pg = pd.read_csv('/home/jc/projeto_b3_linux/3_gold/AWS_b3.csv',sep=';')
#     pg.to_sql('dw_b3',conn,if_exists='append',index=False)

def to_parquet_s3():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('datalake-josecarlos-977711486277')
    prefix_objs = bucket.objects.filter(Prefix="raw-data/fato_b3_")
    prefix_df = []
    df = pd.DataFrame(columns=['fk_empresas', 'cod_listagem', 'date', 'LOADED_DATE','silver_timestamp', 'Papel', 'Cotacao', 'PL', 'PVP', 'PSR', 'DivYield',    'P_Ativo', 'P_CapitalGiro', 'P_Ebit', 'P_Ativ_Circ_Liq', 'Ev_Ebit','Ev_Ebitda', 'Mrg_Ebit', 'Mrg_Ebit.1', 'MrgLiq', 'LiqCorr', 'ROIC','ROE', 'Liq2meses', 'PatrimLiq', 'DivBrutaPatrimonio','CrescReceita5anos'])

    for obj in prefix_objs:
        key = obj.key
        body = obj.get()['Body'].read()
        temp = pd.read_csv(io.BytesIO(body), encoding='utf8',sep=';') 
        df = df.append(temp,ignore_index=False)
    df.to_parquet('s3://datalake-josecarlos-977711486277/parquet_data/',engine='fastparquet',partition_cols='date')

        
