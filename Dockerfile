FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o benthos.tar.gz https://github.com/Jeffail/benthos/releases/download/v4.11.0/benthos_4.11.0_linux_amd64.tar.gz \
    && tar -xzf benthos.tar.gz \
    && mv benthos /usr/local/bin/ \
    && chmod +x /usr/local/bin/benthos \
    && rm benthos.tar.gz

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade bentoml

COPY . .

ENV PYTHONPATH=/app

EXPOSE 3000
RUN chmod +x entrypoint.sh
CMD ["./entrypoint.sh"]