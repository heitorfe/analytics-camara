# Use a imagem base Python slim (baseada em Debian)
FROM python:3.13-slim

# Defina o diretório de trabalho
WORKDIR /app

# Instale as dependências necessárias para o psycopg2 e cron
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copie os arquivos de requisitos e instale as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código da aplicação
COPY . .

# Make the entrypoint script executable
RUN chmod +x /app/docker-entrypoint.sh

# Exponha a porta padrão do Streamlit (8501)
EXPOSE 8501

# Use entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]