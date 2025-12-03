#usa a imagem base oficial do Airflow
FROM apache/airflow:3.1.3

#define usuário
USER airflow

#define o diretório de trabalho no container
WORKDIR /opt/airflow

#instala o Poetry, certificando-se de que está no PATH
RUN pip install poetry

#copia os arquivos de configuração do Poetry
COPY pyproject.toml poetry.lock ./

#instala dependências usando Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --only main