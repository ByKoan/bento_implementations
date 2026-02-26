# -------- STAGE 1: TEST --------
FROM python:3.10-slim AS test

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade bentoml

COPY . .
ENV PYTHONPATH=/app

# Run tests here
RUN pytest 

# -------- STAGE 2: RUNTIME --------
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade bentoml

COPY . .

ENV PYTHONPATH=/app

EXPOSE 3000

CMD ["bentoml", "serve", "api.service:MQTTService", "--host", "0.0.0.0", "--port", "3000"]