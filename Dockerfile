FROM python:3.10-slim

WORKDIR /app

COPY . /app

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 3000

RUN pytest

CMD ["bash", "-c", "python main.py & bentoml serve api.service:svc --host 0.0.0.0 --port 3000"]
