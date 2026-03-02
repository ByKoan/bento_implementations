import logging
from core.utils import build_ingestion_metadata
from core.batch_writer import COLLECTION_READINGS, COLLECTION_URGENT

logger = logging.getLogger(__name__)

BATTERY_THRESHOLD = 15
TEMP_THRESHOLD = 75


class EdgeProcessor:
    def __init__(self, batch_writer):
        self.batch_writer = batch_writer

    def process_reading(self, reading: dict, sensor_type: str, sensor_id: str, agv_id: str):
        value = reading.get("value")

        if value is None:
            logger.warning(f"Sensor {sensor_id} envió valor nulo")
            return None

        # -----------------------------
        # VALORES IMPOSIBLES → URGENT
        # -----------------------------
        if sensor_type == "battery" and (value < 0 or value > 100):
            alert_msg = f"Batería inválida: {value}"
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "battery_invalid",
                "value": alert_msg,
                "timestamp": reading.get("timestamp")
            }

            self.batch_writer.add(alert, sensor_id, collection=COLLECTION_URGENT)
            logger.warning(f"🚨 Enviado a URGENT: {alert_msg}")
            return None

        if sensor_type == "temperature" and (value < -10 or value > 120):
            alert_msg = f"Temperatura inválida: {value}"
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "temperature_invalid",
                "value": alert_msg,
                "timestamp": reading.get("timestamp")
            }

            self.batch_writer.add(alert, sensor_id, collection=COLLECTION_URGENT)
            logger.warning(f"🚨 Enviado a URGENT: {alert_msg}")
            return None

        # -----------------------------
        # ALERTAS NORMALES → URGENT
        # -----------------------------
        if sensor_type == "battery" and value < BATTERY_THRESHOLD:
            alert_msg = f"Batería baja: {value}%"
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "battery_low",
                "value": alert_msg,
                "timestamp": reading.get("timestamp")
            }

            self.batch_writer.add(alert, sensor_id, collection=COLLECTION_URGENT)
            logger.info(f"⚠️ Enviado a URGENT: {alert_msg}")

        if sensor_type == "temperature" and value > TEMP_THRESHOLD:
            alert_msg = f"Sobrecalentamiento: {value}°C"
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "overheat",
                "value": alert_msg,
                "timestamp": reading.get("timestamp")
            }

            self.batch_writer.add(alert, sensor_id, collection=COLLECTION_URGENT)
            logger.warning(f"🔥 Enviado a URGENT: {alert_msg}")

        # -----------------------------
        # LECTURA NORMAL → READINGS
        # -----------------------------
        normal_record = {
            **build_ingestion_metadata(),
            "agv": agv_id,
            "sensor": sensor_id,
            "type": sensor_type,
            "value": value,
            "timestamp": reading.get("timestamp")
        }

        self.batch_writer.add(normal_record, sensor_id, collection=COLLECTION_READINGS)
        logger.info(f"✅ Enviado a READINGS: {value}")

        return normal_record