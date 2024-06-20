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
        
        # Aguardar até que o seletor de colunas seja clicável e clicar nele
        wait = WebDriverWait(driver, 10)
        #colunas_selecionadas = wait.until(EC.element_to_be_clickable((By.ID, 'colunas-ranking__select-button')))
        colunas_selecionadas = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Colunas Selecionadas']")))
        colunas_selecionadas.click()        

        # Aguardar até que a opção "selecionar todas" seja clicável e clicar nela
        #selecionar_todas = wait.until(EC.element_to_be_clickable((By.ID, 'colunas-ranking__todos')))
        selecionar_todas = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Selecionar Todos']")))
        selecionar_todas.click()

        # Aguardar alguns segundos para garantir que todas as colunas sejam carregadas
        time.sleep(5)

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
