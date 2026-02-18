FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 3000

CMD bash -c "python mqtt_listener.py & bentoml serve service:svc --host 0.0.0.0 --port 3000"

