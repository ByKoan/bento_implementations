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
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
MQTT_PUBLISH_TOPIC_ALERTS = os.getenv("MQTT_PUBLISH_TOPIC_ALERTS")

BATTERY_ID = os.getenv("BATTERY_ID")
TEMP_ID = os.getenv("TEMP_ID")
STATUS_ID = os.getenv("STATUS_ID")
HAS_PALLET_ID = os.getenv("HAS_PALLET_ID")

COLLECTION_READINGS = os.getenv("COLLECTION_READINGS")
COLLECTION_URGENT = os.getenv("COLLECTION_URGENT")

logger.info(f"BATTERY_ID cargado: {BATTERY_ID}")
logger.info(f"TEMP_ID cargado: {TEMP_ID}")

# ===============================
# EDGE PROCESSOR
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

        # Automatic timestamp
        if "timestamp" not in payload:
            payload["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"

        # Generación automática de message_id único para trazabilidad
        if "message_id" not in payload:
            payload["message_id"] = str(uuid.uuid4())

        sensor_id = payload["sensor"]

        # ===============================
        # Establish sensor type
        # ===============================
        if sensor_id == BATTERY_ID:
            sensor_type = "battery"
        elif sensor_id == TEMP_ID:
            sensor_type = "temperature"
        elif sensor_id == STATUS_ID:
            sensor_type = "status"
        elif sensor_id == HAS_PALLET_ID:
            sensor_type = "has_pallet"
        else:
            sensor_type = "unknown"

        logger.info(f"Procesando sensor {sensor_id} tipo {sensor_type}")

        # ===============================
        # Proccess readings
        # ===============================
        result = edge_processor.process_reading(
            payload,
            sensor_type=sensor_type,
            sensor_id=sensor_id
        )

        if not result:
            logger.warning(f"EdgeProcessor devolvió None para: {payload}")
            return

        normal_record = result.get("normal_record")
        alerts = result.get("alerts", [])

        # ===============================
        # save alerts (Normal alerts and invalid alerts )
        # ===============================

        # [SOLVED] batch_writer.add is called only once & deleted extracting AGV_ID on edge proccesor for avoiding errors here

        if normal_record and isinstance(normal_record.get("time"), datetime.datetime):
            normal_record["time"] = normal_record["time"].strftime("%Y-%m-%dT%H:%M:%SZ")

        for alert in alerts:
            if isinstance(alert.get("timestamp"), datetime.datetime):
                alert["timestamp"] = alert["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")

        if alerts or normal_record:
            batch_writer.add({"normal_record": normal_record, "alerts": alerts})
            logger.info(f"Enviando a batch_writer: normal_record = {normal_record}, alerts={alerts}")

        for alert in alerts:
            if alert["type"] in ("battery_low", "overheat"):
                try:
                    client.publish(MQTT_PUBLISH_TOPIC_ALERTS, json.dumps(alert))
                    logger.info(f"Publicado en topic {MQTT_PUBLISH_TOPIC_ALERTS}: {alert}")
                except Exception as e:
                    logger.error(f"Error publicando alerta MQTT: {e}")
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