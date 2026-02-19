# Python version
FROM python:3.10-slim

# Set the working directory
WORKDIR /app
COPY . /app

# Set environment variables
ENV PYTHONPATH=/app

# Install dependencies
RUN pip install --upgrade pip
RUN pip install bentoml fastapi uvicorn paho-mqtt pytest

# Run tests
RUN pytest

# Expose the port for the BentoML service
EXPOSE 3000

# Start the MQTT listener and the BentoML service
CMD ["bash", "-c", "python mqtt_listener.py & bentoml serve service:svc --host 0.0.0.0 --port 3000"]
