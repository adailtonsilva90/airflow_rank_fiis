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
from selenium import webdriver


import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


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
            print(f"Arquivo {file['name']} deletado com sucesso.")
        except HttpError as e:
            logger.error(f"Erro ao deletar o arquivo {file['name']}: {e}")

def clean_convert(valor):
    valor_limpo = valor.replace('%', '').replace(',','.').strip()
    return float(valor_limpo)

def filter_fiis(df):
    df['P/VP'] = pd.to_numeric(df['P/VP'])
    df = df[df['P/VP'] < 95 ]

    df['Variação Preço'] = df['Variação Preço'].apply(clean_convert)
    df['Variação Preço'] = pd.to_numeric(df['Variação Preço'])
    df = df[df['Variação Preço'] > 0 ]

    return df

def ETL_send_to_gdrive(file_path_html, file_path_credential, folder_id):
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

            df = df.dropna()
            df = filter_fiis(df)

            # Authenticate to Google Sheets and Google Drive with gspread and google-auth
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(file_path_credential, scopes=scope)
            client = gspread.authorize(credentials)

            # Authenticate to Google Drive
            drive_service = authenticate_drive(file_path_credential)

            # Delete existing sheets with the same name
            delete_existing_spreadsheets(drive_service, 'rank_fiis')
            print("Existing sheets with the name 'rank_fiis' have been deleted.")

            # Create the new spreadsheet in Google Sheets
            spreadsheet = client.create('rank_fiis')
            print("New spreadsheet created in Google Sheets.")
            time.sleep(5)
            
            try:
                move_file_to_folder(drive_service, spreadsheet.id, folder_id)
                print("Spreadsheet moved to specified folder in Google Drive.")

                # Send data from the DataFrame to the spreadsheet
                worksheet = spreadsheet.get_worksheet(0)
                worksheet.update([df.columns.values.tolist()] + df.values.tolist())
                print("DataFrame successfully sent to Google Sheets in the specified folder!")
            except Exception as e:
                print(f"Error moving sheet to folder: {e}")
        else:
            print("Table not found.")
    else:
        print(f"File {file_path_html} not found.")


tsk_ETL_send_to_gdrive = PythonOperator(task_id='tsk_load_table_file_html', 
                                        python_callable=ETL_send_to_gdrive,
                                        op_args=[file_path_html, file_path_credential, folder_id],
                                        provide_context=True,
                                        dag=dag)



tsk_download_file_html >> tsk_ETL_send_to_gdrive