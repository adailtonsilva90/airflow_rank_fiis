import pandas as pd
import os
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator  import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import gspread
from oauth2client.service_account import ServiceAccountCredentials


#carregando variaveis
url = Variable.get('url')
file_path_html = Variable.get('file_path_html')
file_path_credential = Variable.get('file_path_credential')

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
        print("Obtendo dados da url")
        driver.get(url)        
        html_content = driver.page_source

        print("Salvando arquivo html")
        with open(file_path_html, 'w', encoding='utf-8') as file:
            file.write(html_content)
        
    finally:
        driver.quit()


tsk_download_file_html = PythonOperator(task_id='tsk_download_file_html', 
                                    python_callable=dowload_file_html,
                                    op_args=[url, file_path_html],                                      
                                    dag=dag)


def load_table_file_html_to_gdrive(file_path_html, file_path_credential):

    if os.path.exists(file_path_html):        
        with open(file_path_html, 'r', encoding='utf-8') as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        tables = soup.find_all('table')
        if tables:
            df = pd.read_html(str(tables),match='Setor')[0]

            # Autorize o acesso ao Google Sheets
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(file_path_credential, scope)
            client = gspread.authorize(credentials)

            # Encontre a pasta desejada pelo nome
            pasta = next(folder for folder in client.list_folders() if folder['name'] == 'Airflow')
            spreadsheet = client.create('rank_fiis', folder_id=pasta['id'])
            worksheet = spreadsheet.get_worksheet(0)
            dados = df.values.tolist()
            worksheet.append_rows(dados)
            print("DataFrame enviado com sucesso para o Google Sheets na pasta especificada!")
            
        else:
            print("Tabela não encontrada.")
    else:
        print(f"File {file_path_html} does not exist.")

tsk_load_table_file_html_to_gdrive = PythonOperator(task_id='tsk_load_table_file_html', 
                                        python_callable=load_table_file_html_to_gdrive,
                                        op_args=[file_path_html,file_path_credential],
                                        provide_context=True,
                                        dag=dag)



tsk_download_file_html >> tsk_load_table_file_html_to_gdrive