import os
import threading
import time
import logging
import random
from datetime import datetime

from core.pocketbase_client import PocketBaseClient
from core.disk_queue import DiskQueue

logger = logging.getLogger(__name__)

COLLECTION = os.getenv("COLLECTION")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", 5))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
BASE_DELAY = float(os.getenv("BASE_DELAY", 1))
MAX_DELAY = float(os.getenv("MAX_DELAY", 30))

QUEUE_FILE = os.getenv("QUEUE_FILE", "/app/data/pending_readings.log")


class BatchWriter:

    def __init__(self):
        self.buffer = []
        self.pb = PocketBaseClient()
        self.lock = threading.Lock()
        self.running = True
        self.db_available = False

        self.disk = DiskQueue(QUEUE_FILE)

        count = self.disk.count()
        if count:
            print(f"Recuperados {count} registros pendientes en disco.", flush=True)

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    # ===============================
    # PUBLIC METHOD
    # ===============================

    def add(self, ingested: dict, device_id: str):

        record = self._build_record(ingested, device_id)

        with self.lock:
            self.buffer.append(record)

            if len(self.buffer) >= BATCH_SIZE:
                batch = self.buffer[:BATCH_SIZE]
                self.buffer = self.buffer[BATCH_SIZE:]
                self._flush_batch(batch)

    # ===============================
    # RECORD BUILDER
    # ===============================

    def _build_record(self, ingested: dict, sensor_id: str):

        dt = datetime.fromisoformat(
            ingested["ingestion_timestamp"].replace("Z", "+00:00")
        )

        # Redondeo a milisegundos
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

        last_state = None

        while self.running:
            time.sleep(FLUSH_INTERVAL)

            current_state = self._is_db_alive()

            if current_state != last_state:

                if current_state:
                    print("DB volvió. Flushing disco...", flush=True)
                    self._flush_disk()
                else:
                    print("DB cayó.", flush=True)

                last_state = current_state
                self.db_available = current_state

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

            if response.status_code != 200:
                raise Exception(f"Batch HTTP error {response.status_code}")

            results = response.json()

            for idx, item in enumerate(results):
                if item.get("status") == 200:
                    successfully_sent.append(chunk[idx])
                else:
                    raise Exception(f"Batch item failed: {item}")

        return successfully_sent

    # ===============================
    # BATCH INSERT
    # ===============================

    def _flush_batch(self, batch):

        if not batch:
            return

        print(f"FLUSHING {len(batch)} RECORDS", flush=True)

        try:
            self._send_in_chunks(batch)
            logger.info("Inserted %d readings", len(batch))

        except Exception as e:
            logger.error("Batch failed. Error: %s", e)
            self.db_available = False
            self.disk.append(batch)

            # 👇 MUY IMPORTANTE
            raise

    def _flush(self):

        if not self.buffer:
            return

        batch = self.buffer[:]

        try:
            self._flush_batch(batch)

            # SOLO limpiar si fue exitoso
            self.buffer = self.buffer[len(batch):]

        except Exception:
            # No tocar buffer
            pass

    # ===============================
    # DISK FLUSH
    # ===============================

    def _flush_disk(self):

        records = self.disk.load_all()

        if not records:
            return

        print(f"Reintentando {len(records)} registros pendientes...", flush=True)

        try:
            sent_records = self._send_in_chunks(records)

            remaining = [r for r in records if r not in sent_records]

            self.disk.rewrite(remaining)

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