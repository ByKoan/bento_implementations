# Dockerfile para BentoML + MQTT + EdgeProcessor
FROM python:3.10-slim

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Instalar Benthos
RUN curl -L -o benthos.tar.gz https://github.com/Jeffail/benthos/releases/download/v4.11.0/benthos_4.11.0_linux_amd64.tar.gz \
    && tar -xzf benthos.tar.gz \
    && mv benthos /usr/local/bin/ \
    && chmod +x /usr/local/bin/benthos \
    && rm benthos.tar.gz

# Copiar requirements y instalar Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade bentoml

# Copiar todo el código fuente
COPY . .

# Variables de entorno
ENV PYTHONPATH=/app

# Exponer puerto del servicio BentoML
EXPOSE 3000

# CMD para iniciar el servicio directamente, sin entrypoint
CMD ["bentoml", "serve", "api.service:MQTTService", "--host", "0.0.0.0", "--port", "3000"]