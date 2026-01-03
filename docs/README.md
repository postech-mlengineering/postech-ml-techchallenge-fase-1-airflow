# Repositório do Airflow para o Tech Challenge da Fase 1 da Pós-Graduação em Machine Learning Engineering da FIAP

Este repositório cosiste na camada de orquestração desenvolvida para gerenciar o ciclo de vida completo dos dados da aplicação BooksToScrape. O pipeline automatiza o fluxo que alimenta o motor de recomendação da aplicação, garantindo a integridade e a atualização constante dos dados.

### Pré-requisitos

Certifique-se de ter o Python 3.11+ e o Poetry instalados em seu sistema. Para orquestração, recomenda-se o uso do Docker para rodar o ambiente Airflow.

Para instalar o Poetry, use o método oficial:

```bash
curl -sSL [https://install.python-poetry.org](https://install.python-poetry.org) | python3 -
```

### Instalação

Clone o repositório e instale as dependências listadas no pyproject.toml:

```bash
git clone [https://github.com/jorgeplatero/postech-ml-techchallenge-fase-1-airflow.git](https://github.com/jorgeplatero/postech-ml-techchallenge-fase-1-airflow.git)

cd postech-ml-techchallenge-fase-1-airflow

poetry install
```

O Poetry criará um ambiente virtual isolado e instalará todas as bibliotecas necessárias para a execução dos scripts.

### Como Rodar a Aplicação

Para subir o ambiente completo do Airflow (Webserver, Scheduler, Postgres) via Docker:

```bash
docker-compose up -d
```

A API estará rodando em http://localhost:8080. Certifique-se de configurar as variáveis de ambiente necessárias na seção Admin -> Variables da UI.

### Tecnologias

A aplicação atua como o orquestrador que gerencia a ordem e a integridade das operações na API.

| Componente | Tecnologia | Versão (Especificada) | Descrição |
| :--- | :--- | :--- | :--- |
| **Orquestrador** | **Apache Airflow** | `^2.10.0` | Framework para programar, agendar e monitorar fluxos de trabalho. |
| **Linguagem** | **Python** | `>=3.11, <3.14` | Linguagem base para o desenvolvimento dos scripts e operadores da DAG. |
| **Requisições** | **Requests** | `>=2.32.5, <3.0.0` | Cliente HTTP utilizado para interagir com os endpoints da API. |
| **Manipulação Dados** | **Pandas** | `>=2.3.3, <3.0.0` | Biblioteca para processamento e análise de dados tabulares. |
| **Parsing Web** | **BeautifulSoup4** | `>=4.14.2, <5.0.0` | Utilizada para extração de dados de arquivos HTML e XML. |
| **Conectividade DB** | **Airflow Postgres** | `>=6.5.0` | Provider para interação com o banco de dados de metadados. |
| **Ambiente** | **Docker** | `-` | Containerização para garantir paridade entre ambientes. |
| **Gerenciamento** | **Poetry** | `^2.0.0` | Gerenciador de dependências e construção do ambiente. |

### Integrações

A DAG interage diretamente com a API Flask que gerencia o banco de dados e o motor de predição. O repositório da API pode ser acessado em:

Repositório GitHub: https://

### Deploy

Esta API possui arquivo de configuração para Deploy no Vercel. Para realizar o deploy, certifique-se de que o arquivo vercel.json esteja na raiz, apontando para api.py como fonte principal. O Vercel gerenciará o ambiente com base no pyproject.toml.