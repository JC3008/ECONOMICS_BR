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
    
    url = url_string.fundamentus
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}

    req = Request(url, headers = headers)
    response = urlopen(req)
    html = response.read()

    soup = BeautifulSoup(html, 'html.parser')
    soup
    with open(f"{folder.Raw}/output.html", "w", encoding='utf-8') as file:
        file.write(str(soup))

extract_html()
