# Use Python with Debian (better cron support)
FROM python:3.10-bullseye

# Defina o diretório de trabalho
WORKDIR /app

# Instale as dependências necessárias
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copie os arquivos de requisitos e instale as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install APScheduler for task scheduling
RUN pip install --no-cache-dir apscheduler

# Copie o código da aplicação
COPY . .

# Ensure scripts have Unix line endings
RUN dos2unix /app/docker-entrypoint.sh && chmod +x /app/docker-entrypoint.sh

# Use entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]