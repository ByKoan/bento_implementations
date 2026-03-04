# core/edge_processor.py
import os
import logging
import datetime
import dotenv
from core.utils import build_ingestion_metadata

dotenv.load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configuración de límites
BATTERY_MINIMUM_INVALID = int(os.getenv("BATTERY_MINIMUM_INVALID", 0))
BATTERY_MAXIMUM_INVALID = int(os.getenv("BATTERY_MAXIMUM_INVALID", 100))
TEMP_MINIMUM_INVALID = int(os.getenv("TEMP_MINIMUM_INVALID", -50))
TEMP_MAXIMUM_INVALID = int(os.getenv("TEMP_MAXIMUM_INVALID", 150))

BATTERY_THRESHOLD = int(os.getenv("BATTERY_THRESHOLD", 15))
TEMP_THRESHOLD = int(os.getenv("TEMP_THRESHOLD", 75))


class EdgeProcessor:
    """
    Procesa cada lectura de sensor:
    - Valida valores inválidos
    - Genera alertas normales
    - Construye normal_record
    """

    def __init__(self):
        pass

    def process_reading(self, reading: dict, sensor_type: str, sensor_id: str, agv_id: str):
        value = reading.get("value")
        if value is None:
            logger.warning(f"Sensor {sensor_id} envió valor nulo")
            return None

        alerts = []

        # =====================================================
        # Validaciones de valores inválidos
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
            return {"normal_record": None, "alerts": [alert]}

        if sensor_type == "temperature" and (value <= TEMP_MINIMUM_INVALID or value >= TEMP_MAXIMUM_INVALID):
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "temperature_invalid",
                "value": f"Temperatura inválida: {value}",
                "timestamp": reading.get("timestamp")
            }
            logger.warning(f"Temperatura inválida detectada: {value}")
            return {"normal_record": None, "alerts": [alert]}

        if sensor_type == "has_pallet" and value not in (0, 1):
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "has_pallet_invalid",
                "value": f"HasPallet inválido: {value}",
                "timestamp": reading.get("timestamp")
            }
            logger.warning(f"HasPallet inválido detectado: {value}")
            return {"normal_record": None, "alerts": [alert]}

        if sensor_type == "status" and value not in (0, 1, 2, 3):
            alert = {
                **build_ingestion_metadata(),
                "agv": agv_id,
                "sensor": sensor_id,
                "type": "status_invalid",
                "value": f"Status inválido: {value}",
                "timestamp": reading.get("timestamp")
            }
            logger.warning(f"Status inválido detectado: {value}")
            return {"normal_record": None, "alerts": [alert]}

        # =====================================================
        # Alertas normales
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

        # =====================================================
        # Construir registro normal
        # =====================================================
        ts_str = reading.get("timestamp")
        try:
            timestamp = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else datetime.datetime.utcnow()
        except Exception:
            timestamp = datetime.datetime.utcnow()

        normal_record = {
            **build_ingestion_metadata(),
            "agv": agv_id,
            "sensor": sensor_id,
            "type": sensor_type,
            "value": value,
            "time": timestamp.isoformat(),
            "message_id": reading.get("message_id"),
            "_collection": "readings"
        }

        return {"normal_record": normal_record, "alerts": alerts}