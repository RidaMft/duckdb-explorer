FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Systèmes nécessaires pour certains connecteurs (pyodbc), Postgres client, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc g++ \
    unixodbc unixodbc-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code
COPY . .

# Dossiers pour persistance et exports
RUN mkdir -p /app/data /app/exports

EXPOSE 8501

# Lancement Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
