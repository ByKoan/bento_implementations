import logging
import dotenv
import os
import datetime
from core.utils import build_ingestion_metadata

dotenv.load_dotenv()

logger = logging.getLogger(__name__)

BATTERY_MINIMUM_INVALID = int(os.getenv("BATTERY_MINIMUM_INVALID"))
BATTERY_MAXIMUM_INVALID = int(os.getenv("BATTERY_MAXIMUM_INVALID"))
TEMP_MIMIMUM_INVALID = int(os.getenv("TEMP_MIMIMUM_INVALID"))
TEMP_MAXIMUM_INVALID = int(os.getenv("TEMP_MAXIMUM_INVALID"))

BATTERY_THRESHOLD = int(os.getenv("BATTERY_THRESHOLD"))
TEMP_THRESHOLD = int(os.getenv("TEMP_THRESHOLD"))

class EdgeProcessor:
    def __init__(self):
        pass

    def process_reading(self, reading: dict, sensor_type: str, sensor_id: str, agv_id: str):

        value = reading.get("value")

        if value is None:
            logger.warning(f"Sensor {sensor_id} envió valor nulo")
            return None

        alerts = []

        # =====================================================
        # Invalid values
        # =====================================================
        if sensor_type == "battery" and (value <= BATTERY_MINIMUM_INVALID or value >= BATTERY_MAXIMUM_INVALID):
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "battery_invalid",
                "value": f"Batería inválida: {value}",
                "timestamp": reading.get("timestamp")
            }

            logger.warning(f"Battery inválida detectada: {value}")

            return {
                "normal_record": None,
                "alerts": [alert]
            }

        if sensor_type == "temperature" and (value <= TEMP_MIMIMUM_INVALID or value >= TEMP_MAXIMUM_INVALID):
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "temperature_invalid",
                "value": f"Temperatura inválida: {value}",
                "timestamp": reading.get("timestamp")
            }

            logger.warning(f"Temperatura inválida detectada: {value}")

            return {
                "normal_record": None,
                "alerts": [alert]
            }

        # =====================================================
        # Normal alerts
        # =====================================================
        if sensor_type == "battery" and value < BATTERY_THRESHOLD:
            alerts.append({
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "battery_low",
                "value": f"Batería baja: {value}%",
                "timestamp": reading.get("timestamp")
            })

        if sensor_type == "temperature" and value > TEMP_THRESHOLD:
            alerts.append({
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "overheat",
                "value": f"Sobrecalentamiento: {value}°C",
                "timestamp": reading.get("timestamp")
            })

        # ============================
        # Normal record
        # ============================
        ts_str = reading.get("timestamp")
        try:
            if ts_str:
                timestamp = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            else:
                timestamp = datetime.datetime.utcnow()
        except Exception:
            timestamp = datetime.datetime.utcnow()

        normal_record = {
            **build_ingestion_metadata(),
            "agv": agv_id,
            "sensor": sensor_id,
            "type": sensor_type,
            "value": value,
            "time": timestamp.isoformat(),  # ✅ string ISO compatible con date
            # "timestamp" no es necesario si "time" es suficiente
        }

        return {
            "normal_record": normal_record,
            "alerts": alerts
        }