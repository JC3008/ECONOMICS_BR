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
    
    url = url_string.fundamentus
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}

    req = Request(url, headers = headers)
    response = urlopen(req)
    html = response.read()

    soup = BeautifulSoup(html, 'html.parser')
    soup
    with open(f"{folder.Raw}/output.html", "w", encoding='utf-8') as file:
        file.write(str(soup))

def parsing_html():
    '''
    Here the html is stored into a pandas dataframe
    Aqui o arquivo html é carregado para o dataframe
    '''
    url = url_string.fundamentus
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}
    req = Request(url, headers = headers)
    response = urlopen(req)
    html = response.read()

    soup = open(f"{folder.Raw}/output.html", "r")
    soup = BeautifulSoup(html, 'html.parser')
    colunas_names = [col.getText() for col in soup.find('table', {'id': 'resultado'}).find('thead').findAll('th')]
    colunas = {i: col.getText() for i, col in enumerate(soup.find('table', {'id': 'resultado'}).find('thead').findAll('th'))}

    dados = pd.DataFrame(columns=colunas_names)

    for i in range(len(soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr'))):
        linha = soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr')[i].getText().split('\n')[1:]
        inserir_linha = pd.DataFrame(linha).T.rename(columns=colunas)
        dados = pd.concat([dados, inserir_linha], ignore_index=True)
    dados.to_csv(f'{folder.Raw}/raw_fundamentus.csv',sep=';')

def rename_and_insert_loaded_date():
    '''
    Renaming columns and assiging the loaded_date new column
    renomeando colunas e inserindo a coluna loaded_date
    '''
    timestr = dt.today().strftime('%Y-%m-%d')
    dado = pd.read_csv(f'{folder.Raw}/raw_fundamentus.csv',sep=';')
    dado.to_csv(f'{folder.Raw}/hist/hist_{timestr}_raw_fundamentus.csv',sep=';',index=False)
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
    dado.to_csv(f'{folder.Raw}/raw_fundamentus.csv',sep=';',index=False)






