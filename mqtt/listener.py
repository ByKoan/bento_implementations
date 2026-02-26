import os
import json
import logging
import threading
import paho.mqtt.client as mqtt
import datetime

from core.batch_writer import batch_writer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "devices/+/temperature")

# ===============================
# CALLBACKS
# ===============================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Conectado a MQTT broker")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"Suscrito a topic: {MQTT_TOPIC}")
    else:
        logger.error(f"Error al conectar a MQTT broker: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        # Campos mínimos requeridos
        if "sensor" not in payload or "value" not in payload:
            print(f"Mensaje MQTT incompleto: {payload}", flush=True)
            return

        # Rellenar ingestion_timestamp si falta
        if "ingestion_timestamp" not in payload:
            payload["ingestion_timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"

        # Rellenar temp_c si falta
        if "temp_c" not in payload:
            payload["temp_c"] = float(payload["value"])

        batch_writer.add(payload, payload["sensor"])
    except Exception as e:
        print(f"Error procesando mensaje MQTT: {e}", flush=True)

# ===============================
# START LISTENER
# ===============================
def start(batch_writer_instance=None):
    """
    Inicia el cliente MQTT en un hilo aparte y lo conecta al broker.
    """
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # loop_forever corre en el hilo actual, así que lo ponemos en un hilo
    thread = threading.Thread(target=client.loop_forever, daemon=True)
    thread.start()
    logger.info("MQTT listener iniciado en segundo plano")