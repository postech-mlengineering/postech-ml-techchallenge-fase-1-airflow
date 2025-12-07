import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import json


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


API_BASE_URL = 'http://192.168.15.6:5000/'
LOGIN_ENDPOINT = f'{API_BASE_URL}/api/v1/auth/login'
SCRAPE_ENDPOINT = f'{API_BASE_URL}/api/v1/scrape/scrape-and-insert'

API_USERNAME = 'airflow'  
API_PASSWORD = 'postech@01'


def login_to_api(**kwargs):
    '''
    Realiza o login na API, obtém o access token e armazena no XCom.
    '''
    ti = kwargs['ti']
    login_data = {
        'username': API_USERNAME,
        'password': API_PASSWORD
    }
    try:
        response = requests.post(LOGIN_ENDPOINT, json=login_data)
        response.raise_for_status()
        tokens = response.json()
        access_token = tokens.get('access_token')
        ti.xcom_push(key='api_access_token', value=access_token)
        logging.info(f'Login bem-sucedido. Access Token armazenado no XCom.')
    except requests.exceptions.RequestException as e:
        logging.error(e)
        raise

def run_scrape_and_insert(**kwargs):
    '''
    Recupera o access_token do XCom e executa a rota /scrape-and-insert.
    '''
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='login_task', key='api_access_token')
    if not access_token:
        raise
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(SCRAPE_ENDPOINT, headers=headers)
        response.raise_for_status()
        result = response.json()
        print(f'Rota /scrape-and-insert executada com sucesso. Resultados: {json.dumps(result, indent=4)}')
    except requests.exceptions.RequestException as e:
        logging.error(e)
        raise


with DAG(
    dag_id='run_api_web_scraping',
    start_date=datetime(2025, 12, 7),
    schedule='@daily',
    catchup=False,
    tags=['api', 'web scraping'],
    doc_md='''
    DAG para requisição na rota de web scraping da api.
    '''
) as dag:

    task_1 = PythonOperator(
        task_id='login_task',
        python_callable=login_to_api,
    )

    task_2 = PythonOperator(
        task_id='scrape_and_insert_task',
        python_callable=run_scrape_and_insert,
    )

    task_1 >> task_2