import os
import threading
import time
import logging
import random
from core.pocketbase_client import PocketBaseClient

logger = logging.getLogger(__name__)

COLLECTION = os.getenv("COLLECTION")

BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES"))
BASE_DELAY = float(os.getenv("BASE_DELAY"))
MAX_DELAY = float(os.getenv("MAX_DELAY"))


class BatchWriter:

    def __init__(self):
        self.buffer = []
        self.pb = PocketBaseClient()
        self.lock = threading.Lock()
        self.running = True

        self.sensor_cache = {}

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    # ===============================
    # PUBLIC METHOD
    # ===============================

    def add(self, ingested: dict, device_id: str):

        sensor_id = device_id 

        record = self.build_record(ingested, sensor_id)

        with self.lock:
            self.buffer.append(record)

            if len(self.buffer) >= BATCH_SIZE:
                batch = self.buffer[:BATCH_SIZE]
                self.buffer = self.buffer[BATCH_SIZE:]
                self._flush_batch(batch)


    # ===============================
    # SENSOR LOOKUP
    # ===============================

    def _get_sensor_id(self, device_id):

        # cache
        if device_id in self.sensor_cache:
            return self.sensor_cache[device_id]

        print("Buscando sensor:", device_id, flush=True)
        response = self.pb.get(
            f"/api/collections/sensors/records",
            params={"filter": f'device_id="{device_id}"'}
        )
        print("Respuesta:", response.json(), flush=True)

        items = response.json().get("items", [])

        if not items:
            return None

        sensor_id = items[0]["id"]

        self.sensor_cache[device_id] = sensor_id

        return sensor_id

    # ===============================
    # RECORD BUILDER
    # ===============================

    def build_record(self, ingested: dict, sensor_id: str):
        timestamp = ingested["ingestion_timestamp"].replace("+00:00", "Z")

        return {
            "sensor": sensor_id,
            "time": timestamp,
            "value": float(ingested["temp_c"])
        }

    # ===============================
    # BACKGROUND LOOP
    # ===============================

    def _run(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL)
            with self.lock:
                if self.buffer:
                    self._flush()

    def _flush(self):
        batch = self.buffer[:]
        self.buffer.clear()
        self._flush_batch(batch)

    # ===============================
    # BATCH INSERT (histórico real)
    # ===============================

    def _flush_batch(self, batch):

        print(f"\nFLUSHING {len(batch)} RECORDS\n", flush=True)

        requests = []

        for record in batch:
            requests.append({
                "method": "POST",
                "url": f"/api/collections/{COLLECTION}/records",
                "body": record
            })

        payload = {"requests": requests}

        self._send_with_retry(payload)

        logger.info("Inserted %d readings", len(batch))

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

                return response

            except Exception as e:
                attempt += 1

                if attempt >= MAX_RETRIES:
                    logger.error("Max retries reached. Giving up.")
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
