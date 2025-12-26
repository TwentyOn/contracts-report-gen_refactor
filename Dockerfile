# тест образа для работы драйвера google
FROM python:3.11-slim AS builder

WORKDIR /app

# зависимости для google-chrome
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gpg \
    && rm -rf /var/lib/apt/lists/*

# google-chrome для работы selenium
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# COPY generate_screenshots_refactored.py database_manager.py minio_client.py .
COPY . .
CMD ["python3", "-u", "main_processor.py"]
# CMD ["python3", "-u", "generate_screenshots_refactored.py"]