from bs4 import *
import pandas as pd
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime as dt
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sqlalchemy
from html.parser import HTMLParser
import locale
import pymysql
import psycopg2
import time
import re
from connections import *


def extract_html():
    '''
    This piece of code perform the download of html file from fundamentus.com table
    Aqui o html é baixado do site fundamentus.com
    '''
    
    url = string.url_fundamentus
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}

    req = Request(url, headers = headers)
    response = urlopen(req)
    html = response.read()

    soup = BeautifulSoup(html, 'html.parser')
    soup
    with open(f"{string.folder_raw}/output.html", "w", encoding='utf-8') as file:
        file.write(str(soup))

def parsing_html():
    '''
    Here the html is stored into a pandas dataframe
    Aqui o arquivo html é carregado para o dataframe
    '''
    url = string.url_fundamentus
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}
    req = Request(url, headers = headers)
    response = urlopen(req)
    html = response.read()

    soup = open(f"{string.folder_raw}/output.html", "r")
    soup = BeautifulSoup(html, 'html.parser')
    colunas_names = [col.getText() for col in soup.find('table', {'id': 'resultado'}).find('thead').findAll('th')]
    colunas = {i: col.getText() for i, col in enumerate(soup.find('table', {'id': 'resultado'}).find('thead').findAll('th'))}

    dados = pd.DataFrame(columns=colunas_names)

    for i in range(len(soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr'))):
        linha = soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr')[i].getText().split('\n')[1:]
        inserir_linha = pd.DataFrame(linha).T.rename(columns=colunas)
        dados = pd.concat([dados, inserir_linha], ignore_index=True)
    dados.to_csv(f'{string.folder_raw}/raw_fundamentus.csv',sep=';')

def rename_and_insert_loaded_date():
    '''
    Renaming columns, assigning the loaded_date new column and saving the previous file as historic
    renomeando colunas e inserindo a coluna loaded_date e guardando o arquivo como historico
    '''
    timestr = dt.today().strftime('%Y-%m-%d')
    dado = pd.read_csv(f'{string.folder_Raw}/raw_fundamentus.csv',sep=';')
    dado.to_csv(f'{string.folder_Raw}/hist/hist_{timestr}_raw_fundamentus.csv',sep=';',index=False)
    dado.rename(columns={'Cotação':'Cotacao',
    'P/L':'PL',
    'P/VP':'PVP',
    'Div.Yield':'DivYield',
    'P/Ativo':'P_Ativo',
    'P/Cap.Giro':'P_CapitalGiro',
    'P/EBIT':'P_Ebit',
    'P/Ativ Circ.Liq':'P_Ativ_Circ_Liq',
    'EV/EBIT':'Ev_Ebit',
    'EV/EBITDA':'Ev_Ebitda',
    'Mrg Ebit' : 'Mrg_Ebit',
    'Mrg. Líq.':'MrgLiq',
    'Liq. Corr.':'LiqCorr',
    'Liq.2meses':'Liq2meses',
    'Patrim. Líq':'PatrimLiq',
    'Dív.Brut/ Patrim.':'DivBrutaPatrimonio',
    'Cresc. Rec.5a':'CrescReceita5anos'}
    , inplace = True)
    time = dt.now()    
    dado['LOADED_DATE'] = time
    dado = dado[['Papel','Cotacao','PL','PVP','PSR','DivYield','P_Ativo','P_CapitalGiro','P_Ebit','P_Ativ_Circ_Liq','Ev_Ebit','Ev_Ebitda','Mrg_Ebit','MrgLiq','LiqCorr','ROIC','ROE','Liq2meses','PatrimLiq','DivBrutaPatrimonio','CrescReceita5anos','LOADED_DATE']]
    dado.to_csv(f'{string.folder_Raw}/raw_fundamentus.csv',sep=';',index=False)


def save_csv_curated():

    query_silver = '''
select 
b."pk_empresas" as "fk_empresas",
b."cod_listagem" as "cod_listagem",
a."LOADED_DATE",
a."Papel",
a."Cotacao",
a."PL",
a."PVP",
a."PSR",
a."DivYield",
a."P_Ativo",
a."P_CapitalGiro",
a."P_Ebit",
a."P_Ativ_Circ_Liq",
a."Ev_Ebit",
a."Ev_Ebitda",
a."Mrg_Ebit",
a."MrgLiq",
a."LiqCorr",
a."ROIC",
a."ROE",
a."Liq2meses",
a."PatrimLiq",
a."DivBrutaPatrimonio",
a."CrescReceita5anos"

FROM bronze_fundamentus a
left join dim_empresas b on a."Papel" = b."ticker"
'''

    conn_string = string.cnx_string
    db = create_engine(conn_string)
    conn = db.connect()
    dados = pd.read_sql(query_silver,conn)
    dados['date'] = dados['LOADED_DATE'].dt.date
    time = dt.now()
    dados['silver_timestamp'] = time
    
    dados['fk_empresas'] = dados['fk_empresas'].fillna(999999)
    dados['fk_empresas'] = dados['fk_empresas'].astype('int')
    
    dados['DivYield'] = dados['DivYield'].apply(lambda x: float(str(x).replace('%','').replace('.','').replace(',','.'))/100)
    dados['ROIC'] = dados['ROIC'].apply(lambda x: float(str(x).replace('%','').replace('.','').replace(',','.'))/100)
    dados['ROE'] = dados['ROE'].apply(lambda x: float(str(x).replace('%','').replace('.','').replace(',','.'))/100)
    dados['CrescReceita5anos'] = dados['CrescReceita5anos'].apply(lambda x: float(str(x).replace('%','').replace('.','').replace(',','.'))/100)

    dados['Cotacao'] = dados['Cotacao'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['PL'] = dados['PL'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['PVP'] = dados['PVP'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['PSR'] = dados['PSR'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['P_Ativo'] = dados['P_Ativo'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['P_CapitalGiro'] = dados['P_CapitalGiro'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['P_Ebit'] = dados['P_Ebit'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['P_Ativ_Circ_Liq'] = dados['P_Ativ_Circ_Liq'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['Ev_Ebit'] = dados['P_Ativ_Circ_Liq'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['Ev_Ebitda'] = dados['P_Ativ_Circ_Liq'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['Mrg Ebit'] = dados['P_Ativ_Circ_Liq'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['MrgLiq'] = dados['P_Ativ_Circ_Liq'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['LiqCorr'] = dados['LiqCorr'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['Liq2meses'] = dados['Liq2meses'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['PatrimLiq'] = dados['PatrimLiq'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))
    dados['DivBrutaPatrimonio'] = dados['DivBrutaPatrimonio'].apply(lambda x: float(str(str(x).replace('.','')).replace(',','.')))

    dados = dados[['fk_empresas','cod_listagem','date','LOADED_DATE','silver_timestamp','Papel', 'Cotacao', 'PL', 'PVP', 'PSR', 'DivYield', 'P_Ativo',
    'P_CapitalGiro', 'P_Ebit', 'P_Ativ_Circ_Liq', 'Ev_Ebit', 'Ev_Ebitda',
    'Mrg_Ebit','Mrg_Ebit', 'MrgLiq', 'LiqCorr', 'ROIC', 'ROE', 'Liq2meses',
    'PatrimLiq', 'DivBrutaPatrimonio', 'CrescReceita5anos']]

    dados.to_csv(f'{string.folder_curated}/silver_fundamentus.csv',sep=';',index=False)

save_csv_curated()



