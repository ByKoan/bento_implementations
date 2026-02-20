import os
import threading
import time
import logging
import random
import json
from datetime import datetime

from core.pocketbase_client import PocketBaseClient

logger = logging.getLogger(__name__)

COLLECTION = os.getenv("COLLECTION")

BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES"))
BASE_DELAY = float(os.getenv("BASE_DELAY"))
MAX_DELAY = float(os.getenv("MAX_DELAY"))

QUEUE_FILE = os.getenv("QUEUE_FILE", "/app/data/pending_readings.log")


class BatchWriter:

    def __init__(self):
        self.buffer = []
        self.pb = PocketBaseClient()
        self.lock = threading.Lock()
        self.running = True
        self.db_available = False  # ahora arranca en falso hasta comprobar

        os.makedirs(os.path.dirname(QUEUE_FILE), exist_ok=True)

        # Cargar pendientes en reinicio
        self._load_from_disk()

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    # ===============================
    # PUBLIC METHOD
    # ===============================

    def add(self, ingested: dict, device_id: str):

        record = self.build_record(ingested, device_id)

        with self.lock:
            self.buffer.append(record)

            if len(self.buffer) >= BATCH_SIZE:
                batch = self.buffer[:BATCH_SIZE]
                self.buffer = self.buffer[BATCH_SIZE:]
                self._flush_batch(batch)

    # ===============================
    # RECORD BUILDER
    # ===============================

    def build_record(self, ingested: dict, sensor_id: str):
        dt = datetime.fromisoformat(
            ingested["ingestion_timestamp"].replace("Z", "+00:00")
        )

        # redondear a milisegundos
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)

        return {
            "sensor": sensor_id,
            "time": dt.isoformat().replace("+00:00", "Z"),
            "value": float(ingested["temp_c"])
        }

    # ===============================
    # BACKGROUND LOOP
    # ===============================

    def _run(self):

        last_state = None  # None al inicio

        while self.running:
            time.sleep(FLUSH_INTERVAL)

            current_state = self._is_db_alive()

            # Si el estado cambió → log
            if current_state != last_state:

                if current_state:
                    print("DB volvió. Flushing disco...", flush=True)
                    self._flush_disk()
                else:
                    print("DB cayó.", flush=True)

                last_state = current_state
                self.db_available = current_state

            # Si la DB está viva → flush normal
            if current_state:
                with self.lock:
                    if self.buffer:
                        self._flush()

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
    # CORE CHUNK SENDER
    # ===============================

    def _send_in_chunks(self, records):

        successfully_sent = []

        for i in range(0, len(records), BATCH_SIZE):
            chunk = records[i:i + BATCH_SIZE]

            print(f"Sending chunk of {len(chunk)} records", flush=True)

            requests = [{
                "method": "POST",
                "url": f"/api/collections/{COLLECTION}/records",
                "body": record
            } for record in chunk]

            payload = {"requests": requests}

            response = self._send_with_retry(payload)

            # Verificación real
            if response.status_code != 200:
                raise Exception(f"Batch HTTP error {response.status_code}")

            results = response.json()

            # Verificar cada item del batch
            for idx, item in enumerate(results):
                if item.get("status") == 200:
                    successfully_sent.append(chunk[idx])
                else:
                    raise Exception(f"Batch item failed: {item}")

        return successfully_sent

    # ===============================
    # BATCH INSERT
    # ===============================

    def _flush_batch(self, batch, from_disk=False):

        if not batch:
            return

        print(f"FLUSHING {len(batch)} RECORDS", flush=True)

        try:
            sent_records = self._send_in_chunks(batch)
            logger.info("Inserted %d readings", len(batch))

        except Exception as e:
            logger.error("Batch failed. Error: %s", e)
            self.db_available = False

            # SOLO guardar en disco si NO vienen del disco
            if not from_disk:
                self._append_to_disk(batch)

    def _flush(self):
        batch = self.buffer[:]
        self.buffer.clear()
        self._flush_batch(batch)

    # ===============================
    # DISK MANAGEMENT
    # ===============================

    def _append_to_disk(self, batch):
        with open(QUEUE_FILE, "a") as f:
            for record in batch:
                f.write(json.dumps(record) + "\n")

    def _load_from_disk(self):
        if not os.path.exists(QUEUE_FILE):
            return
        with open(QUEUE_FILE, "r") as f:
            count = sum(1 for line in f if line.strip())

        print(f"Recuperados z<{count} registros pendientes en disco.", flush=True)

    def _flush_disk(self):

        if not os.path.exists(QUEUE_FILE):
            return

        with open(QUEUE_FILE, "r") as f:
            records = [json.loads(line.strip()) for line in f if line.strip()]

        if not records:
            return

        print(f"Reintentando {len(records)} registros pendientes...", flush=True)

        try:
            sent_records = self._send_in_chunks(records)

            # Reescribir archivo SOLO con los que NO se enviaron
            remaining = [r for r in records if r not in sent_records]

            with open(QUEUE_FILE, "w") as f:
                for record in remaining:
                    f.write(json.dumps(record) + "\n")

            print(f"Pendientes restantes en disco: {len(remaining)}", flush=True)

        except Exception as e:
            logger.error("Falló flush_disk: %s", e)

    # ===============================
    # RETRY
    # ===============================

    def _send_with_retry(self, payload):

        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                response = self.pb.post("/api/batch", payload)

                if response.status_code >= 500:
                    raise Exception(f"Server error {response.status_code}")

                if response.status_code == 400:
                    # Ignorar duplicados (idempotencia básica)
                    if "validation_not_unique" in response.text:
                        logger.warning("Duplicate detected. Skipping.")
                        return response
                    raise Exception(f"Bad request: {response.text}")

                return response

            except Exception as e:
                attempt += 1

                if attempt >= MAX_RETRIES:
                    raise

                delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                jitter = random.uniform(0, 0.5)
                sleep_time = delay + jitter

                logger.warning(
                    "Retry %d/%d in %.2f seconds due to: %s",
                    attempt,
                    MAX_RETRIES,
                    sleep_time,
                    e,
                )

                time.sleep(sleep_time)


batch_writer = BatchWriter()