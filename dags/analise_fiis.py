import pandas as pd
import os
import time
import logging
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator  import PythonOperator


from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Loading Variables
url = Variable.get('url')
file_path_html = Variable.get('file_path_html')
file_path_credential = Variable.get('file_path_credential')
folder_id = Variable.get('folder_id')

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

def authenticate_drive(file_path_credential):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file(file_path_credential, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
    return service

def move_file_to_folder(drive_service, file_id, folder_id):
    # Get the current parents of the file
    file = drive_service.files().get(fileId=file_id, fields='parents').execute()
    previous_parents = ",".join(file.get('parents', []))  # Adiciona valor padrão se 'parents' não existir

    # Move the file to the new folder
    file = drive_service.files().update(
        fileId=file_id,
        addParents=folder_id,
        removeParents=previous_parents,
        fields='id, parents'
    ).execute()
    return file

def delete_existing_spreadsheets(drive_service, file_name):
    # Search for the file with the given name
    query = f"name = '{file_name}' and mimeType = 'application/vnd.google-apps.spreadsheet'"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])

    for file in files:
        try:
            drive_service.files().delete(fileId=file['id']).execute()
            logger.info(f"Arquivo {file['name']} deletado com sucesso.")
        except HttpError as e:
            logger.error(f"Erro ao deletar o arquivo {file['name']}: {e}")

def load_table_file_html_to_gdrive(file_path_html, file_path_credential, folder_id):
    logger.info("Verificando existência do arquivo HTML...")
    if os.path.exists(file_path_html):
        with open(file_path_html, 'r', encoding='utf-8') as file:
            html_content = file.read()
            logger.info("Conteúdo do arquivo HTML lido com sucesso.")

        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')

        if tables:
            logger.info("Tabelas encontradas no HTML.")
            df = pd.read_html(str(tables), match='Setor')[0]
            logger.info("DataFrame criado a partir das tabelas.")

            # Handling non-JSON compatible values
            df = df.replace([float('inf'), float('-inf'), pd.NA, None], 0)

            # Authenticate to Google Sheets and Google Drive with gspread and google-auth
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(file_path_credential, scopes=scope)
            client = gspread.authorize(credentials)

            # Authenticate to Google Drive
            drive_service = authenticate_drive(file_path_credential)

            # Delete existing sheets with the same name
            delete_existing_spreadsheets(drive_service, 'rank_fiis')
            logger.info("Planilhas existentes com o nome 'rank_fiis' foram deletadas.")

            # Create the new spreadsheet in Google Sheets
            spreadsheet = client.create('rank_fiis')
            logger.info("Nova planilha criada no Google Sheets.")
            time.sleep(5)  # Esperar alguns segundos para garantir que a planilha seja criada
            
            try:
                # Move spreadsheet to specific folder in Google Drive
                move_file_to_folder(drive_service, spreadsheet.id, folder_id)
                logger.info("Planilha movida para a pasta especificada no Google Drive.")

                # Send data from the DataFrame to the spreadsheet
                worksheet = spreadsheet.get_worksheet(0)
                worksheet.update([df.columns.values.tolist()] + df.values.tolist())
                logger.info("DataFrame enviado com sucesso para o Google Sheets na pasta especificada!")
            except HttpError as e:
                logger.error(f"Erro ao mover a planilha para a pasta: {e}")
        else:
            logger.info("Tabela não encontrada.")
    else:
        logger.info(f"Arquivo {file_path_html} não encontrado.")


tsk_load_table_file_html_to_gdrive = PythonOperator(task_id='tsk_load_table_file_html', 
                                        python_callable=load_table_file_html_to_gdrive,
                                        op_args=[file_path_html, file_path_credential, folder_id],
                                        provide_context=True,
                                        dag=dag)



tsk_download_file_html >> tsk_load_table_file_html_to_gdrive