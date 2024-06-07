import pandas as pd
import os
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator  import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

#carregando variaveis
url = Variable.get('url')
file_path_html = Variable.get('file_path_html')

dag= DAG('rank_fiis', schedule_interval=None, catchup=False,start_date=datetime(2024, 6 ,4))


def dowload_file_html(url, file_path_html):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument('--remote-debugging-port=9222')

    driver = webdriver.Remote(
        command_executor='http://chromedriver:4444/wd/hub',
        options=chrome_options
    )

    try:
        driver.get(url)        
        html_content = driver.page_source
        
        with open(file_path_html, 'w', encoding='utf-8') as file:
            file.write(html_content)
        
    finally:
        driver.quit()


tsk_download_file_html = PythonOperator(task_id='tsk_download_file_html', 
                                    python_callable=dowload_file_html,
                                    op_args=[url, file_path_html],                                      
                                    dag=dag)


def load_table_file_html(file_path_html):

    if os.path.exists(file_path_html):        
        with open(file_path_html, 'r', encoding='utf-8') as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        tables = soup.find_all('table')
        if tables:
            df = pd.read_html(str(tables),match='Setor')[0]
            print(df.head(5))
        else:
            print("Tabela não encontrada.")
    else:
        print(f"File {file_path_html} does not exist.")

tsk_load_table_file_html = PythonOperator(task_id='tsk_load_table_file_html', 
                                        python_callable=load_table_file_html,
                                        op_args=[file_path_html],
                                        provide_context=True,
                                        dag=dag)



tsk_download_file_html >> tsk_load_table_file_html