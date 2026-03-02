# core/batch_writer.py
import os
import json
import threading
import time
import logging
from datetime import datetime

from core.pocketbase_client import PocketBaseClient
from core.disk_queue import DiskQueue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

COLLECTION_READINGS = os.getenv("COLLECTION_READINGS", "readings")
COLLECTION_URGENT = os.getenv("COLLECTION_URGENT", "urgent_alerts")
MQTT_ERROR_TOPIC = os.getenv("MQTT_ERROR_TOPIC")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
BASE_DELAY = float(os.getenv("BASE_DELAY", 0.5))
MAX_DELAY = float(os.getenv("MAX_DELAY", 5))
QUEUE_FILE = os.getenv("QUEUE_FILE", "batch_queue.json")


class BatchWriter:
    """
    BatchWriter guarda los registros en disco y los sube a PocketBase en batches.
    Se asegura que no haya duplicados por `message_id`.
    """

    def __init__(self, mqtt_client=None):
        self.mqtt_client = mqtt_client
        self.lock = threading.Lock()
        self.running = True

        self.pb = PocketBaseClient()
        self.disk = DiskQueue(QUEUE_FILE)

        count = self.disk.count()
        if count:
            logger.info(f"Recuperados {count} registros pendientes en disco.")

        self.disk_thread = threading.Thread(target=self._disk_retry_loop, daemon=True)
        self.disk_thread.start()

    # ===============================
    # PUBLIC: Agregar registro
    # ===============================
    def add(self, ingested: dict, sensor_id: str, collection: str = None):
        collection = collection or COLLECTION_READINGS
        with self.lock:
            payload = ingested.copy()
            payload["_collection"] = collection
            # Evitar duplicados
            if not self.disk.exists(payload.get("message_id")):
                self.disk.append([payload])
                logger.info(f"✅ Registro añadido al disco en {collection}: {payload}")
            else:
                logger.info(f"⚠ Registro duplicado ignorado: {payload['message_id']}")

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

            # Procesar batches
            for i in range(0, len(disk_records), BATCH_SIZE):
                batch = disk_records[i:i + BATCH_SIZE]
                sent_records = self._send_with_retry_batch(batch)
                # Eliminar del disco los que se enviaron correctamente
                with self.lock:
                    current_disk = self.disk.load_all()
                    remaining = [
                        r for r in current_disk
                        if r.get("message_id") not in {s.get("message_id") for s in sent_records}
                    ]
                    self.disk.rewrite(remaining)

    # ===============================
    # DB Health Check
    # ===============================
    def _is_db_alive(self):
        try:
            return self.pb.get("/api/health").status_code == 200
        except Exception:
            return False

    # ===============================
    # MQTT Error
    # ===============================
    def _send_to_error_topic(self, record, reason):
        if not self.mqtt_client:
            logger.error("MQTT client no disponible")
            return
        payload = {
            "record": record,
            "reason": str(reason),
            "failed_at": datetime.utcnow().isoformat() + "Z"
        }
        try:
            self.mqtt_client.publish(MQTT_ERROR_TOPIC, json.dumps(payload), qos=1)
            logger.error("Registro enviado a error topic")
        except Exception as e:
            logger.critical("No se pudo publicar en error topic: %s", e)

    # ===============================
    # Enviar batch con retries
    # ===============================
    def _send_with_retry_batch(self, batch):
        # Filtrar duplicados por message_id
        unique_batch = {r['message_id']: r for r in batch}.values()
        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                successfully_sent = []

                for r in unique_batch:
                    collection = r.get("_collection")
                    if not collection:
                        logger.error(f"Registro sin colección válida: {r}")
                        continue

                    payload = {
                        "requests": [
                            {
                                "method": "POST",
                                "url": f"/api/collections/{collection}/records",
                                "body": {k: v for k, v in r.items() if k != "_collection"},
                            }
                        ]
                    }

                    response = self.pb.post("/api/batch", payload)

                    if response.status_code in (200, 201):
                        successfully_sent.append(r)
                    else:
                        logger.error(f"Error enviando registro a {collection}: {response.text}")
                        self._send_to_error_topic(r, response.text)

                return successfully_sent

            except Exception as e:
                attempt += 1
                if attempt < MAX_RETRIES:
                    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                    logger.warning(f"Retry {attempt} en {delay}s")
                    time.sleep(delay)
                else:
                    logger.error("Max retries alcanzado")
                    for r in unique_batch:
                        self._send_to_error_topic(r, "max_retries_exceeded")
                    return list(unique_batch)


# Crear instancia global
batch_writer = BatchWriter()