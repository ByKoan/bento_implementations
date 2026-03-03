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

BATTERY_ID = os.getenv("BATTERY_ID")
TEMP_ID = os.getenv("TEMP_ID")

COLLECTION_READINGS = os.getenv("COLLECTION_READINGS")
COLLECTION_URGENT = os.getenv("COLLECTION_URGENT")

logger.info(f"BATTERY_ID cargado: {BATTERY_ID}")
logger.info(f"TEMP_ID cargado: {TEMP_ID}")

# ===============================
# EDGE PROCESSOR (NO escribe en DB)
# ===============================
edge_processor = EdgeProcessor()

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

        # Timestamp automático
        if "timestamp" not in payload:
            payload["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"

        # message_id automático
        if "message_id" not in payload:
            payload["message_id"] = str(uuid.uuid4())

        sensor_id = payload["sensor"]
        agv_id = payload.get("agv_id", "unknown")

        # ===============================
        # Determinar tipo de sensor
        # ===============================
        if sensor_id == BATTERY_ID:
            sensor_type = "battery"
        elif sensor_id == TEMP_ID:
            sensor_type = "temperature"
        else:
            sensor_type = "unknown"

        logger.info(f"Procesando sensor {sensor_id} tipo {sensor_type}")

        # ===============================
        # Procesar lectura
        # ===============================
        result = edge_processor.process_reading(
            payload,
            sensor_type=sensor_type,
            sensor_id=sensor_id,
            agv_id=agv_id
        )

        if not result:
            logger.warning(f"EdgeProcessor devolvió None para: {payload}")
            return

        normal_record = result.get("normal_record")
        alerts = result.get("alerts", [])

        # ===============================
        # Guardar alertas
        # ===============================
        for alert in alerts:
            batch_writer.add(alert, sensor_id, collection=COLLECTION_URGENT)
            logger.warning(f"🚨 Enviado a URGENT: {alert}")

        # ===============================
        # Guardar lectura normal SOLO si existe
        # ===============================
        if normal_record:
            batch_writer.add(normal_record, sensor_id, collection=COLLECTION_READINGS)
            logger.info(f"✅ Enviado a READINGS: {normal_record}")

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