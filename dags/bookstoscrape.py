from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
from typing import List, Dict, Any, Optional
import os



DAG_ID = 'bookstoscrape'
POSTGRES_CONN_ID = 'postgresql_conn_id'
FILE_PATH = '/opt/airflow/data/books.csv'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = 'http://books.toscrape.com/'
HOME_URL = BASE_URL + 'index.html'


def clean_currency(currency_str: str) -> float:
    '''
    Limpa a string de preço, removendo caracteres de moeda (£, etc.) 
    e convertendo para float.
    '''
    cleaned_str = currency_str.replace('Â', '').replace('£', '')
    try:
        return float(cleaned_str)
    except ValueError:
        logging.warning(f'Não foi possível converter o preço: {currency_str} para float. Retornando 0.0')
        return 0.0


def extract_number_from_availability(availability_text: str) -> int:
    '''Extrai o número de estoque disponível (e.g., 22 de 'In stock (22 available)').'''
    match = re.search(r'\d+', availability_text)
    return int(match.group()) if match else 0


def get_category_links() -> List[Dict[str, str]]:
    '''Coleta o nome e a URL inicial de todas as categorias na página inicial.'''
    logging.info("Iniciando a coleta de links de categorias...")

    try:
        home_response = requests.get(HOME_URL, timeout=10)
        home_response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        logging.error(f'Erro ao acessar a URL inicial: {e}')
        return []
    
    home_soup = BeautifulSoup(home_response.text, 'html.parser')
    
    #encontra a lista de categorias
    category_list_html = home_soup.find('ul', class_='nav nav-list').find('ul').find_all('li')
    
    categories = []
    for item in category_list_html:
        tag_a = item.find('a')
        category_name = tag_a.text.strip()
        relative_link = tag_a['href']
        
        #constrói a url absoluta
        category_url = BASE_URL + relative_link 
        
        categories.append({
            'name': category_name,
            'initial_url': category_url
        })
        
    logging.info(f'Total de {len(categories)} categorias encontradas.')
    return categories


def extract_book_details(url: str, genre: str) -> Optional[Dict[str, Any]]:
    '''Acessa a página de detalhes de um livro e extrai todas as informações.'''
    try:
        detail_response = requests.get(url, timeout=10)
        detail_response.raise_for_status()
        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')

        #extraindo dados simples
        title = detail_soup.find('h1').text
        
        #preço principal
        price = detail_soup.find('p', class_='price_color').text
        price = clean_currency(price)

        #descrição
        description_tag = detail_soup.find('div', id='product_description')
        description = description_tag.find_next_sibling('p').text if description_tag else 'No Description'

        #avaliação em estrelas
        rating_tag = detail_soup.find('p', class_='star-rating')
        rating = rating_tag['class'][1] if rating_tag else 'No Rating'

        #tabela de informações
        product_table = detail_soup.find('table', class_='table-striped').find_all('td')
        
        #extração de dados da tabela
        upc = product_table[0].text
        product_type = product_table[1].text
        
        price_excl_tax = clean_currency(product_table[2].text)
        price_incl_tax = clean_currency(product_table[3].text)
        tax = clean_currency(product_table[4].text)

        availability_text = product_table[5].text
        availability = extract_number_from_availability(availability_text)
        
        number_of_reviews = int(product_table[6].text) if product_table[6].text.isdigit() else 0

        return {
            'title': title,
            'genre': genre,
            'price': price,
            'upc': upc,
            'product_type': product_type,
            'price_excl_tax': price_excl_tax,
            'price_incl_tax': price_incl_tax,
            'tax': tax,
            'number_of_reviews': number_of_reviews,
            'availability': availability,
            'rating': rating,
            'description': description,
            'url': url
        }
    except Exception as e:
        logging.error(f'Erro ao extrair detalhes de {url}: {e}')
        return None


def scrape_category(category: Dict[str, str], data_list: List[Dict[str, Any]]) -> None:
    '''Itera sobre todas as páginas de uma categoria, extrai os links e os detalhes dos livros.'''
    genre_name = category['name']
    current_url = category['initial_url']
    page_number = 1

    logging.info(f'Scraping gênero: {genre_name}')

    while True:
        logging.info(f'  > Processando {genre_name} - pag. {page_number}')

        try:
            page_response = requests.get(current_url, timeout=15)
            page_response.raise_for_status()
            page_soup = BeautifulSoup(page_response.text, 'html.parser')
            
            #encontrar todos os livros na página atual
            books_on_page = page_soup.find_all('article', class_='product_pod')
            
            #iterar sobre os links de livros
            for book in books_on_page:
                relative_link = book.find('h3').find('a')['href']
                #ajusta o link relativo para ser absoluto, removendo o padrão "../../"
                url = BASE_URL + 'catalogue/' + relative_link.replace('../', '')
                #extrair e adicionar os detalhes
                book_data = extract_book_details(url, genre_name)
                if book_data:
                    data_list.append(book_data)
            
            #verificar Paginação ("next" button)
            next_button = page_soup.find('li', class_='next')
            
            if next_button:
                #se houver botão 'next', atualiza a URL para a próxima página
                link_next = next_button.find('a')['href']
                
                #constrói a URL completa para a próxima página
                url_parts = current_url.split('/')
                #Garante que o link de paginação é anexado corretamente ao diretório da categoria
                if 'page-' in url_parts[-1]:
                    current_url = '/'.join(url_parts[:-1]) + '/' + link_next
                else: #Caso seja a primeira página (index.html), substitui
                    current_url = current_url.replace('index.html', link_next)
                
                page_number += 1
            else:
                #não há mais páginas, sai do loop
                break

        except requests.exceptions.RequestException as e:
            logging.error(f'Erro ao processar a página {current_url}: {e}')
            break
        except Exception as e:
            logging.error(f'Erro inesperado ao raspar categoria {genre_name}: {e}')
            break

def scrape_data(file_path: str):
    '''
    Executa a lógica de raspagem, coleta todos os dados e salva em um arquivo CSV.
    '''
    data = []

    #coleta todos os links de gênero
    categories_list = get_category_links()
    
    #processa cada gênero
    for category in categories_list:
        scrape_category(category, data)
        
    logging.info(f'\nTotal de {len(data)} livros coletados.')

    #criação do dataframe
    if data:
        #cria o dataframe
        df = pd.DataFrame(data)
        #colunas ordenadas para o output final
        ordered_columns = [
            'upc', 
            'title', 
            'genre', 
            'price', 
            'availability', 
            'rating', 
            'description',
            'product_type', 
            'price_excl_tax', 
            'price_incl_tax', 
            'tax', 
            'number_of_reviews',
            'url'
        ]
        df = df[ordered_columns]
        logging.info("DataFrame criado com sucesso!")
        
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info(f'Dados salvos em "{file_path}"')
    else:
        logging.warning("Nenhum dado de livro foi coletado.")
        #cria um arquivo csv vazio
        pd.DataFrame(columns=[
            'title', 'genre', 'price', 'availability', 'rating', 'upc', 
            'description', 'product_type', 'price_excl_tax', 
            'price_incl_tax', 'tax', 'number_of_reviews', 'url'
        ]).to_csv(file_path, index=False, encoding='utf-8')


def load_csv_to_postgres(file_path: str, conn_id: str, table_name: str):
    if not os.path.exists(file_path):
        logging.error(f'O arquivo CSV não foi encontrado no caminho: {file_path}')
        raise FileNotFoundError(f'CSV file not found: {file_path}')

    logging.info(f'Lendo dados do CSV em {file_path}')
    df = pd.read_csv(file_path)

    if df.empty:
        logging.warning("O DataFrame está vazio. Nenhuma inserção será feita.")
        return

    pg_conn = PostgresHook(postgres_conn_id=conn_id)
    logging.info(f'Conexão com PostgreSQL estabelecida (ID: {conn_id})')

    table_schema = {
        'title': 'VARCHAR(500)', 'genre': 'VARCHAR(100)', 'price': 'NUMERIC(10, 2)',
        'availability': 'INTEGER', 'rating': 'VARCHAR(50)', 'upc': 'VARCHAR(50)',
        'description': 'TEXT', 'product_type': 'VARCHAR(50)', 'price_excl_tax': 'NUMERIC(10, 2)',
        'price_incl_tax': 'NUMERIC(10, 2)', 'tax': 'NUMERIC(10, 2)',
        'number_of_reviews': 'INTEGER', 'url': 'VARCHAR(1024)'
    }

    columns_with_types = [f'"{col}" {table_schema.get(col, "TEXT")}' for col in df.columns]
    columns = list(df.columns)
    
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", \n".join(columns_with_types)}, PRIMARY KEY ("upc"));'
    
    logging.info(f'Garantindo a existência e restrições da tabela "{table_name}"...')
    pg_conn.run(create_table_sql)
    logging.info(f'Tabela "{table_name}" verificada com sucesso.')

    truncate_sql = f'TRUNCATE TABLE {table_name} RESTART IDENTITY;'
    logging.info(f'Truncando a tabela "{table_name}"...')
    pg_conn.run(truncate_sql)

    rows = [tuple(row) for row in df.values]

    logging.info(f'Iniciando a inserção de {len(rows)} linhas na tabela "{table_name}"...')
    
    pg_conn.insert_rows(
        table=table_name,
        rows=rows,
        target_fields=columns
    )
    
    logging.info('Dados inseridos com sucesso (Truncate and Load).')


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 12, 1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['webscraping', 'bookstoscrape',],
    doc_md='''
    DAG para raspar o site books.toscrape.com, salvar os dados em um CSV e carregá-los no PostgreSQL.
    '''
) as dag:
    
    task_1 = PythonOperator(
        task_id='scrape_books_to_csv',
        python_callable=scrape_data,
        op_kwargs={'file_path': FILE_PATH},
        doc_md='''
        Esta task executa todo o scraping do site books.toscrape.com, coleta detalhes de todos os livros
        e salva o resultado em um arquivo CSV no caminho temporário especificado.
        '''
    )

    task_2 = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'file_path': FILE_PATH,
            'conn_id': POSTGRES_CONN_ID,
            'table_name': 'books'
        },
        doc_md='''
        Esta task lê o CSV gerado, conecta-se ao PostgreSQL, cria a tabela 'books' e insere todos os dados do CSV.
        '''
    )

    task_1 >> task_2
