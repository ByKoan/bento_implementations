# core/batch_writer.py
import os
import json
import threading
import time
import logging
import requests
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
BASE_DELAY = float(os.getenv("BASE_DELAY", 1))
MAX_DELAY = float(os.getenv("MAX_DELAY", 10))
QUEUE_FILE = os.getenv("QUEUE_FILE", "./queue.json")
BENTHOS_URL = os.getenv("BENTHOS_URL", "http://benthos:4197/ingest")


class BatchWriter:
    """
    Guarda registros en disco y los sube a PocketBase en batches vía Benthos 4.27.
    Filtra normal_record = None y envía alerts a otra colección.
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
    def add(self, processed: dict):
        """
        processed: dict devuelto por EdgeProcessor
        {
            "normal_record": {...} or None,
            "alerts": [...]
        }
        """
        with self.lock:
            # Guardar normal_record si existe
            normal_record = processed.get("normal_record")
            if normal_record:
                normal_record["_collection"] = COLLECTION_READINGS
                if not self.disk.exists(normal_record.get("message_id")):
                    self.disk.append([normal_record])
                    logger.info(f"✅ normal_record añadido al disco: {normal_record}")
                else:
                    logger.info(f"⚠ normal_record duplicado ignorado: {normal_record.get('message_id')}")

            # Guardar alerts si existen
            alerts = processed.get("alerts", [])
            for alert in alerts:
                alert["_collection"] = COLLECTION_URGENT
                if not self.disk.exists(alert.get("message_id")):
                    self.disk.append([alert])
                    logger.info(f"🚨 alerta añadida al disco: {alert}")
                else:
                    logger.info(f"⚠ alerta duplicada ignorada: {alert.get('message_id')}")

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
        # Quitar duplicados por message_id
        unique_batch_dict = {r['message_id']: r for r in batch}
        unique_batch = list(unique_batch_dict.values())
        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                # Enviar batch completo como string JSON a Benthos
                payload_str = json.dumps(unique_batch, default=str)
                response = requests.post(
                    BENTHOS_URL,
                    data=payload_str,
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                if response.status_code in (200, 201):
                    logger.info(f"✅ Batch enviado a Benthos ({len(unique_batch)} registros)")
                    return unique_batch
                else:
                    logger.error(
                        "Error enviando a Benthos: %s %s",
                        response.status_code,
                        response.text
                    )
                    attempt += 1
                    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                    time.sleep(delay)
            except Exception as e:
                attempt += 1
                delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                logger.warning(f"Retry {attempt} a Benthos en {delay}s: {e}")
                time.sleep(delay)

        # Si max retries, enviar todos a error topic
        for r in unique_batch:
            self._send_to_error_topic(r, "max_retries_exceeded")
        return unique_batch


# Crear instancia global
batch_writer = BatchWriter()