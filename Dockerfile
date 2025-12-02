#usa a imagem base oficial do Airflow
FROM apache/airflow:3.1.3

#instalação do Poetry e Dependências ---

#mudar para o usuário root para instalar ferramentas globais
USER root

# Instala o Poetry, certificando-se de que está no PATH
RUN pip install poetry

#mudar de volta para o usuário airflow para a instalação de dependências
USER airflow

#definir o diretório de trabalho no container
WORKDIR /opt/airflow

#copiar os arquivos de configuração do Poetry
COPY pyproject.toml poetry.lock ./

#instalar dependências usando Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-dev