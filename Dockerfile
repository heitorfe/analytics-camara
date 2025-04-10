# Use a imagem base do Python
FROM python:3.13-alpine

# Defina o diretório de trabalho
WORKDIR /app

# Copie os arquivos de requisitos e instale as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código da aplicação
COPY . .

# Exponha a porta padrão do Streamlit (8501)
EXPOSE 8501

# Comando para rodar o Streamlit
CMD ["streamlit", "run", "app/dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]