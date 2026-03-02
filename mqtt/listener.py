import os
import json
import logging
import threading
import paho.mqtt.client as mqtt
import datetime
import uuid

from core.batch_writer import batch_writer
from core.edge_proccesor import EdgeProcessor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ===============================
# ENV VARIABLES
# ===============================
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

BATTERY_ID = os.getenv("BATTERY_ID")  # Sensor de batería
TEMP_ID = os.getenv("TEMP_ID")        # Sensor de temperatura

logger.info(f"BATTERY_ID cargado: {BATTERY_ID}")
logger.info(f"TEMP_ID cargado: {TEMP_ID}")

# ===============================
# EDGE PROCESSOR
# ===============================
edge_processor = EdgeProcessor(batch_writer)

# ===============================
# CALLBACKS MQTT
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

        if "sensor" not in payload or "value" not in payload:
            logger.warning(f"Mensaje MQTT incompleto: {payload}")
            return

        if "timestamp" not in payload:
            payload["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"

        # Agregar message_id si no viene
        if "message_id" not in payload:
            payload["message_id"] = str(uuid.uuid4())

        sensor_id = payload["sensor"]
        agv_id = payload.get("agv_id", "unknown")

        collection_readings = os.getenv("COLLECTION_READINGS", "readings")
        collection_urgent = os.getenv("COLLECTION_URGENT", "urgent_alerts")

        # Determinar tipo de sensor
        if sensor_id == BATTERY_ID:
            sensor_type = "battery"
        elif sensor_id == TEMP_ID:
            sensor_type = "temperature"
        else:
            sensor_type = "unknown"

        # Procesar lectura
        processed_payload = edge_processor.process_reading(
            payload,
            sensor_type=sensor_type,
            sensor_id=sensor_id,
            agv_id=agv_id
        )

        if not processed_payload:
            logger.warning(f"EdgeProcessor devolvió None para: {payload}")
            return

        # ===============================
        # FILTRO DE URGENCIAS E INVALIDOS
        # ===============================
        value = processed_payload.get("value")
        is_invalid = False
        is_urgent = False

        if sensor_type == "battery":
            if value is None or not (0 <= value <= 100):
                is_invalid = True
            elif value < 20:
                is_urgent = True

        elif sensor_type == "temperature":
            if value is None or not (-50 <= value <= 100):
                is_invalid = True
            elif value > 75:
                is_urgent = True

        # ===============================
        # DECISIÓN DE COLECCIÓN
        # ===============================
        if is_urgent or is_invalid:
            batch_writer.add(processed_payload, sensor_id, collection=collection_urgent)
            logger.warning(f"🔥 Enviado a URGENT/INVALID: {processed_payload}")
            return  # <- clave: no sigue a readings

        # Solo envía a readings si no es urgente ni inválido
        batch_writer.add(processed_payload, sensor_id, collection=collection_readings)
        logger.info(f"✅ Enviado a READINGS: {processed_payload}")

    except Exception as e:
        logger.error(f"Error procesando mensaje MQTT: {e}")


# ===============================
# START LISTENER
# ===============================
def start(batch_writer_instance=None):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    thread = threading.Thread(target=client.loop_forever, daemon=True)
    thread.start()

    logger.info("MQTT listener iniciado en segundo plano")