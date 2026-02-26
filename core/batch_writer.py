import os
import threading
import time
import logging
import random
import json
from datetime import datetime

from core.pocketbase_client import PocketBaseClient
from core.disk_queue import DiskQueue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

COLLECTION = os.getenv("COLLECTION", "default_collection")
MQTT_ERROR_TOPIC = os.getenv("MQTT_ERROR_TOPIC", "errors/topic")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
BASE_DELAY = float(os.getenv("BASE_DELAY", 1))
MAX_DELAY = float(os.getenv("MAX_DELAY", 10))

QUEUE_FILE = os.getenv("QUEUE_FILE")


class BatchWriter:
    """Solo buffer en disco, no hay buffer en memoria"""

    def __init__(self, mqtt_client=None):
        self.mqtt_client = mqtt_client
        self.lock = threading.Lock()
        self.running = True

        self.pb = PocketBaseClient()
        self.disk = DiskQueue(QUEUE_FILE)

        count = self.disk.count()
        if count:
            logger.info(f"Recuperados {count} registros pendientes en disco.")

        # Hilo que sube registros del disco a PocketBase
        self.disk_thread = threading.Thread(target=self._disk_retry_loop, daemon=True)
        self.disk_thread.start()

    # ===============================
    # PUBLIC: agregar registro directo al disco
    # ===============================
    def add(self, ingested: dict, sensor_id: str):
        record = self._build_record(ingested, sensor_id)
        with self.lock:
            self.disk.append([record])  # siempre va directo al disco

    # ===============================
    # RECORD BUILDER
    # ===============================
    def _build_record(self, ingested: dict, sensor_id: str):
        dt = datetime.fromisoformat(ingested["ingestion_timestamp"].replace("Z", "+00:00"))
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
        return {"sensor": sensor_id, "time": dt.isoformat().replace("+00:00", "Z"), "value": float(ingested["temp_c"])}

    # ===============================
    # LOOP DISCO -> DB
    # ===============================
    def _disk_retry_loop(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL)
            with self.lock:
                disk_records = self.disk.load_all()

            if not disk_records:
                continue

            if not self._is_db_alive():
                logger.warning("DB caída, esperando para subir registros del disco...")
                continue

            # Subimos en batches
            for i in range(0, len(disk_records), BATCH_SIZE):
                batch = disk_records[i:i + BATCH_SIZE]
                sent_records = self._send_with_retry_batch(batch)

                with self.lock:
                    current_disk = self.disk.load_all()
                    remaining = [r for r in current_disk if r not in sent_records]
                    self.disk.rewrite(remaining)

    # ===============================
    # HEALTH CHECK
    # ===============================
    def _is_db_alive(self):
        try:
            return self.pb.get("/api/health").status_code == 200
        except Exception:
            return False

    # ===============================
    # ERROR MQTT
    # ===============================
    def _send_to_error_topic(self, record, reason):
        if not self.mqtt_client:
            logger.error("MQTT client no disponible")
            return
        payload = {"record": record, "reason": str(reason), "failed_at": datetime.utcnow().isoformat() + "Z"}
        try:
            self.mqtt_client.publish(MQTT_ERROR_TOPIC, json.dumps(payload), qos=1)
            logger.error("Registro enviado a error topic")
        except Exception as e:
            logger.critical("No se pudo publicar en error topic: %s", e)

    # ===============================
    # SEND CON REINTENTOS POR BATCH
    # ===============================
    def _send_with_retry_batch(self, batch):
        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                payload = {"requests": [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": r} for r in batch]}
                response = self.pb.post("/api/batch", payload)

                successfully_sent = []
                if response.status_code == 200:
                    results = response.json()
                    for idx, item in enumerate(results):
                        if item.get("status") in [200, 201, 400]:
                            successfully_sent.append(batch[idx])
                        else:
                            self._send_to_error_topic(batch[idx], item)
                            successfully_sent.append(batch[idx])
                    return successfully_sent
                elif response.status_code >= 500:
                    raise Exception("Server error")
                return []
            except Exception:
                attempt += 1
                if attempt < MAX_RETRIES:
                    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY) + random.uniform(0, 0.5)
                    time.sleep(delay)
                else:
                    # Si falla MAX_RETRIES, enviamos a error y los consideramos procesados
                    for r in batch:
                        self._send_to_error_topic(r, "max_retries_exceeded")
                    return batch  # marcamos como "procesados" para limpiar del disco

# Instancia global
batch_writer = BatchWriter()