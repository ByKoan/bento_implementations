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

COLLECTION = os.getenv("COLLECTION")
MQTT_ERROR_TOPIC = os.getenv("MQTT_ERROR_TOPIC")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", 5))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
BASE_DELAY = float(os.getenv("BASE_DELAY", 1))
MAX_DELAY = float(os.getenv("MAX_DELAY", 30))

QUEUE_FILE = os.getenv("QUEUE_FILE", "/app/data/pending_readings.log")


class BatchWriter:

    def __init__(self, mqtt_client=None):
        self.mqtt_client = mqtt_client
        self.buffer = []
        self.lock = threading.Lock()
        self.running = True
        self.processing_disk = False

        self.pb = PocketBaseClient()
        self.disk = DiskQueue(QUEUE_FILE)

        count = self.disk.count()
        if count:
            print(f"Recuperados {count} registros pendientes en disco.", flush=True)

        # Hilo de buffer
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        # Hilo de retry disco
        self.disk_thread = threading.Thread(target=self._disk_retry_loop, daemon=True)
        self.disk_thread.start()

    # ===============================
    # PUBLIC
    # ===============================
    def add(self, ingested: dict, device_id: str):
        record = self._build_record(ingested, device_id)
        with self.lock:
            if not self.processing_disk:
                self.buffer.append(record)
            else:
                self.disk.append([record])

    # ===============================
    # RECORD BUILDER
    # ===============================
    def _build_record(self, ingested: dict, sensor_id: str):
        dt = datetime.fromisoformat(ingested["ingestion_timestamp"].replace("Z", "+00:00"))
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
        return {"sensor": sensor_id, "time": dt.isoformat().replace("+00:00", "Z"), "value": float(ingested["temp_c"])}

    # ===============================
    # BUFFER LOOP
    # ===============================
    def _run(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL)
            if not self._is_db_alive():
                with self.lock:
                    if self.buffer:
                        print(f"DB caída. Guardando {len(self.buffer)} registros en disco...", flush=True)
                        self.disk.append(self.buffer)
                        self.buffer.clear()
                continue

            while True:
                with self.lock:
                    if not self.buffer:
                        break
                    batch = self.buffer[:BATCH_SIZE]
                    self.buffer = self.buffer[BATCH_SIZE:]
                self._flush_batch(batch)

    # ===============================
    # DISK RETRY LOOP
    # ===============================
    def _disk_retry_loop(self):
        while self.running:
            with self.lock:
                disk_records = self.disk.load_all()
                self.processing_disk = bool(disk_records)

            if disk_records:
                total_records = len(disk_records)
                start_index = 0
                while start_index < total_records and self.running:
                    batch = disk_records[start_index:start_index + BATCH_SIZE]
                    attempt = 0
                    while attempt < MAX_RETRIES and self.running:
                        if not self._is_db_alive():
                            delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY) + random.uniform(0, 0.5)
                            print(f"DB caída, retry batch en {delay:.2f}s...", flush=True)
                            time.sleep(delay)
                            attempt += 1
                            continue

                        try:
                            print(f"Intentando batch {start_index}-{start_index + len(batch)} intento {attempt + 1}/{MAX_RETRIES}", flush=True)
                            sent = self._send_with_retry_batch(batch)
                            remaining = [r for r in batch if r not in sent]

                            with self.lock:
                                # Recargamos todos los registros que quedaron fuera del batch actual
                                all_disk = self.disk.load_all()
                                # Reescribimos disco sin los registros enviados
                                self.disk.rewrite([r for r in all_disk if r not in batch] + remaining)

                            start_index += len(batch)
                            break
                        except Exception as e:
                            attempt += 1
                            delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY) + random.uniform(0, 0.5)
                            if attempt <= MAX_RETRIES:
                                print(f"Retry {attempt}/{MAX_RETRIES} batch en {delay:.2f}s...", flush=True)
                            else:
                                print(f"Batch alcanzó MAX_RETRIES, enviando a error topic...", flush=True)
                                for r in batch:
                                    self._send_to_error_topic(r, "max_retries_exceeded")
                                with self.lock:
                                    all_disk = self.disk.load_all()
                                    self.disk.rewrite([r for r in all_disk if r not in batch])
                                break
                            time.sleep(delay)

            with self.lock:
                self.processing_disk = False
            time.sleep(FLUSH_INTERVAL)

    # ===============================
    # HEALTH CHECK
    # ===============================
    def _is_db_alive(self):
        try:
            response = self.pb.get("/api/health")
            return response.status_code == 200
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
    # SEND WITH RETRIES POR BATCH
    # ===============================
    def _send_with_retry_batch(self, batch):
        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                payload = {"requests": [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": r} for r in batch]}
                response = self.pb.post("/api/batch", payload)

                if response.status_code >= 500:
                    raise Exception(f"Server error {response.status_code}")

                successfully_sent = []
                if response.status_code == 200:
                    results = response.json()
                    for idx, item in enumerate(results):
                        if item.get("status") == 200:
                            successfully_sent.append(batch[idx])
                        else:
                            self._send_to_error_topic(batch[idx], item)
                elif response.status_code == 400:
                    for record in batch:
                        single_payload = {"requests": [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": record}]}
                        r = self.pb.post("/api/batch", single_payload)
                        if r.status_code == 200:
                            successfully_sent.append(record)
                        else:
                            self._send_to_error_topic(record, r.text)
                return successfully_sent
            except Exception:
                attempt += 1
                if attempt < MAX_RETRIES:
                    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY) + random.uniform(0, 0.5)
                    time.sleep(delay)
                else:
                    raise

    # ===============================
    # FLUSH BATCH
    # ===============================
    def _flush_batch(self, batch):
        if not batch:
            return
        print(f"FLUSHING {len(batch)} RECORDS", flush=True)
        try:
            sent = self._send_with_retry_batch(batch)
            remaining = [r for r in batch if r not in sent]
            if remaining:
                self.disk.append(remaining)
            logger.info("Inserted %d readings", len(sent))
        except Exception as e:
            logger.error("Batch failed: %s", e)
            self.disk.append(batch)

    # ===============================
    # FLUSH DISCO MANUAL
    # ===============================
    def _flush_disk(self):
        records = self.disk.load_all()
        if not records:
            return
        print(f"Reintentando {len(records)} pendientes del disco...", flush=True)
        try:
            sent = self._send_with_retry_batch(records)
            remaining = [r for r in records if r not in sent]
            self.disk.rewrite(remaining)
            print(f"Pendientes restantes en disco: {len(remaining)}", flush=True)
        except Exception as e:
            logger.error("Falló flush_disk: %s", e)


batch_writer = BatchWriter()