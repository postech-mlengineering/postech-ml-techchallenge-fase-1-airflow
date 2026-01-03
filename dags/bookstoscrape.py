import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_BASE_URL = 'http://192.168.15.9:5000'
LOGIN_ENDPOINT = f'{API_BASE_URL}/api/v1/auth/login'
SCRAPE_ENDPOINT = f'{API_BASE_URL}/api/v1/scrape'
TRAINING_ENDPOINT = f'{API_BASE_URL}/api/v1/ml/training-data' 

API_USERNAME = Variable.get('api_username')
API_PASSWORD = Variable.get('api_password')


def login_to_api(**kwargs):
    ti = kwargs['ti']
    login_data = {'username': API_USERNAME, 'password': API_PASSWORD}
    logging.info(f'Tentando login na URL: {LOGIN_ENDPOINT}')
    try:
        response = requests.post(LOGIN_ENDPOINT, json=login_data, timeout=30)
        response.raise_for_status()
        tokens = response.json()
        access_token = tokens.get('access_token')
        if access_token:
            ti.xcom_push(key='api_access_token', value=access_token)
            logging.info('Login bem-sucedido.')
        else:
            raise ValueError('Token não encontrado na resposta da API.')
    except requests.exceptions.RequestException as e:
        logging.error(f'Erro no login: {e.response.text if e.response else e}')
        raise


def run_scrape(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='login', key='api_access_token')
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    
    logging.info(f'Iniciando Scraping: {SCRAPE_ENDPOINT}')
    response = requests.post(SCRAPE_ENDPOINT, headers=headers, timeout=600)
    response.raise_for_status()
    logging.info('Scraping finalizado com sucesso.')


def run_training_data(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='login', key='api_access_token')
    headers = {'Authorization': f'Bearer {access_token}'}
    
    logging.info(f'Iniciando Processamento de ML (GET): {TRAINING_ENDPOINT}')

    try:
        response = requests.get(TRAINING_ENDPOINT, headers=headers, timeout=1800)
        response.raise_for_status()
        result = response.json()
        logging.info(f'Treinamento concluído. Artefatos gerados: {result.get("artifacts_saved")}')
    except requests.exceptions.RequestException as e:
        logging.error(f'Erro no pipeline de ML: {e}')
        raise


with DAG(
    dag_id='bookstoscrape',
    start_date=datetime(2025, 12, 9),
    schedule='@daily',
    catchup=False,
    tags=['api', 'scraping', 'machine-learning'],
    doc_md='''
    Esta DAG orquestra o ciclo completo de dados para o motor de recomendação:
    '''
) as dag:
    
    task_login = PythonOperator(task_id='login', python_callable=login_to_api)
    task_scrape = PythonOperator(task_id='run_scrape', python_callable=run_scrape)
    task_ml = PythonOperator(task_id='run_training_data', python_callable=run_training_data)

    task_login >> task_scrape >> task_ml