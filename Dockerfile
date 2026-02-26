FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade bentoml

COPY . .

ENV PYTHONPATH=/app

EXPOSE 3000

# Run tests here
RUN pytest 

CMD ["bentoml", "serve", "api.service:MQTTService", "--host", "0.0.0.0", "--port", "3000"]