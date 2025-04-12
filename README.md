# Câmara dos Deputados - Análise de Dados

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) que coleta dados da API da Câmara dos Deputados, transforma os dados e os carrega em um banco de dados PostgreSQL para análise posterior.

## 📋 Requisitos

- Python 3.10+
- Docker e Docker Compose
- PostgreSQL

## 🔧 Configuração

### Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```
# Configuração do banco de dados
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=camara_analytics
DATABASE_URL=postgresql://postgres:postgres@db:5432/camara_analytics
```

### Instalação Local (sem Docker)

1. Clone o repositório:
```bash
git clone https://github.com/heitorfe/analytics-camara.git
cd analytics-camara
```

2. Crie um ambiente virtual e instale as dependências:
```bash
python -m venv .venv
source .venv/bin/activate  # No Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure o banco de dados PostgreSQL localmente e execute:
```bash
python -m app.database.create_tables
```

### Instalação com Docker

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/analytics-camara.git
cd analytics-camara
```

2. Inicie os containers:
```bash
docker-compose up -d
```

3. Inicialize o banco de dados e execute a primeira ingestão:
```bash
docker-compose exec app bash -c "./docker-entrypoint.sh init-db"
```

## 🚀 Executando o ETL

### Modos de Ingestão

O projeto suporta dois modos de ingestão:

- **Full**: Realiza uma ingestão completa dos dados, substituindo os dados existentes.
- **Incremental**: Realiza uma ingestão apenas dos dados novos, preservando os dados existentes.

### Entidades Disponíveis

- **deputados**: Informações básicas sobre os deputados
- **votacoes**: Sessões de votação na Câmara
- **votos**: Votos individuais dos deputados em cada votação
- **all**: Todas as entidades acima

### Executando Localmente

```bash
# Ingestão full de todas as entidades
python -m app.ingestion.cron_etl --mode full --entity all

# Ingestão incremental de uma entidade específica
python -m app.ingestion.cron_etl --mode incremental --entity votacoes

# Ingestão com intervalo de datas específico
python -m app.ingestion.cron_etl --mode full --entity votacoes --start-date 2023-01-01 --end-date 2023-01-31

# Ingestão dos últimos N dias
python -m app.ingestion.cron_etl --mode incremental --entity all --days 7
```

### Executando com Docker

```bash
# Ingestão full de todas as entidades
docker-compose exec app bash -c "python -m app.ingestion.cron_etl --mode full --entity all"

# Ingestão incremental de uma entidade específica
docker-compose exec app bash -c "python -m app.ingestion.cron_etl --mode incremental --entity votacoes"
```

## ⏱️ Agendamento de Tarefas

O projeto está configurado para executar as seguintes tarefas automaticamente:

- **Diariamente às 2:00**: Execução incremental para todas as entidades
- **Aos domingos às 3:00**: Execução completa para todas as entidades

Você pode modificar o agendamento editando o arquivo `crontab` na raiz do projeto.

## 📊 Estrutura de Dados

### Datasets Brutos

Os dados brutos são armazenados no diretório `data/raw` nos seguintes arquivos:

- `deputados.parquet`: Informações básicas dos deputados
- `detalhes_deputados.parquet`: Informações detalhadas dos deputados
- `votacoes.parquet`: Sessões de votação
- `votos.parquet`: Votos individuais dos deputados

### Datasets Processados

Os dados transformados são armazenados no diretório `data/processed`.

### Banco de Dados

A estrutura do banco de dados está definida no arquivo `app/database/DDL.sql` e inclui as seguintes tabelas:

- `deputados`: Informações sobre os deputados
- `votacoes`: Sessões de votação
- `votos`: Votos dos deputados nas sessões

## 🔍 Acesso aos Dados

Você pode acessar os dados do PostgreSQL usando qualquer cliente SQL:

```bash
# Acesso direto via psql (local)
psql -h localhost -U postgres -d camara_analytics

# Acesso via Docker
docker-compose exec db psql -U postgres -d camara_analytics
```

## 📝 Logging

Os logs do ETL são armazenados no diretório `logs/` e podem ser visualizados para acompanhar o progresso das ingestões e diagnosticar problemas.

## 🛠️ Desenvolvimento

### Estrutura do Projeto

```
analytics-camara/
├── app/
│   ├── config.py               # Configurações gerais
│   ├── database/               # Configuração do banco de dados
│   │   ├── DDL.sql             # Schema do banco
│   │   ├── models.py           # Modelos SQLAlchemy
│   │   └── ...
│   ├── ingestion/              # Pipeline de ETL
│   │   ├── api.py              # Funções de API
│   │   ├── extract.py          # Tarefas de extração
│   │   ├── transform_tasks.py  # Tarefas de transformação
│   │   ├── load.py             # Tarefas de carregamento
│   │   ├── flow.py             # Fluxos Prefect
│   │   ├── cron_etl.py         # Script para execução agendada
│   │   └── ...
│   └── models/                 # Modelos de dados
├── data/                       # Armazenamento de dados
│   ├── raw/                    # Dados brutos
│   └── processed/              # Dados processados
├── logs/                       # Logs de execução
├── notebooks/                  # Notebooks de análise
├── docker-compose.yml          # Configuração do Docker
├── Dockerfile                  # Configuração da imagem
├── crontab                     # Configuração de agendamento
├── requirements.txt            # Dependências
└── README.md                   # Este arquivo
```