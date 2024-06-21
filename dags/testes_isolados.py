import pandas as pd
import os
import time
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator  import PythonOperator

from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import gspread

from google.oauth2.service_account import Credentials
#from googleapiclient.discovery import build


#Loading variables
url = Variable.get('url')
file_path_html = Variable.get('file_path_html')
file_path_credential = Variable.get('file_path_credential')

dag= DAG('testes_isolados', schedule_interval=None, catchup=False,start_date=datetime(2024, 6 ,4))

def clean_convert(valor):
    valor_limpo = valor.replace('%', '').replace(',','.').strip()
    return float(valor_limpo)
    

def filter_fiis(df):
    df = df.dropna()
    df['P/VP'] = pd.to_numeric(df['P/VP'])
    df = df[df['P/VP'] < 95 ]

    df['Variação Preço'] = df['Variação Preço'].apply(clean_convert)
    df['Variação Preço'] = pd.to_numeric(df['Variação Preço'])
    df = df[df['Variação Preço'] > 0 ]


    return df

def ETL_send_to_gdrive(file_path_html):
    print("Checking existence of the HTML file...")
    if os.path.exists(file_path_html):
        with open(file_path_html, 'r', encoding='utf-8') as file:
            html_content = file.read()
            print("HTML file content read successfully.")

        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')

        if tables:
            print("Tables found in HTML.")
            df = pd.read_html(str(tables), match='Setor')[0]
            print("DataFrame created from tables.")
          
            df = filter_fiis(df)
            print(df.head())
            print(f"Toltal de linhas: {df.shape[0]}")


tsk_teste = PythonOperator(task_id='tsk_teste', 
                                    python_callable=ETL_send_to_gdrive,
                                    op_args=[file_path_html],                                      
                                    dag=dag)
                                    
tsk_teste
