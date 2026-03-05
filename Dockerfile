FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade bentoml

COPY . .

ENV PYTHONPATH=/app

EXPOSE 3000

RUN pytest

CMD ["bentoml", "serve", "api.service:MQTTService", "--host", "0.0.0.0", "--port", "3000"]