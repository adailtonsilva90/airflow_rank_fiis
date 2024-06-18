import pandas as pd
import os
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator  import PythonOperator

from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import gspread
#from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


#Loading variables
url = Variable.get('url')
file_path_html = Variable.get('file_path_html')
file_path_credential = Variable.get('file_path_credential')

dag= DAG('testes_isolados', schedule_interval=None, catchup=False,start_date=datetime(2024, 6 ,4))


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
        print("Getting data of url")
        driver.get(url)        
        html_content = driver.page_source

        print("Saving file html")
        with open(file_path_html, 'w', encoding='utf-8') as file:
            file.write(html_content)
        
    finally:
        driver.quit()


tsk_download_file_html = PythonOperator(task_id='tsk_download_file_html', 
                                    python_callable=dowload_file_html,
                                    op_args=[url, file_path_html],                                      
                                    dag=dag)
                                    
tsk_download_file_html
