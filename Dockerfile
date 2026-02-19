FROM python:3.10-slim

WORKDIR /app

COPY . /app

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 3000

RUN pytest

CMD ["bentoml", "serve", "api.service:MQTTService", "--host", "0.0.0.0", "--port", "3000"]





