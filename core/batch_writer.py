import os
import threading
import time
import requests
import logging
from core.pocketbase_client import PocketBaseClient

logger = logging.getLogger(__name__)

POCKETBASE_URL = os.getenv("POCKETBASE_URL")
COLLECTION = os.getenv('COLLECTION')

BATCH_SIZE = int(os.getenv('BATCH_SIZE'))
FLUSH_INTERVAL = int(os.getenv('FLUSH_INTERVAL'))

class BatchWriter:

    def __init__(self):
        self.buffer = []
        self.pb = PocketBaseClient()
        self.lock = threading.Lock()
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def add(self, ingested: dict, sensor_id: str):
        record = self.build_record(ingested, sensor_id)

        print("RECORD CONSTRUIDO:", record, flush=True)

        with self.lock:
            self.buffer.append(record)
            if len(self.buffer) >= BATCH_SIZE:
                self._flush()

    def _run(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL)
            with self.lock:
                if self.buffer:
                    self._flush()

    def build_record(self, ingested: dict, sensor_id: str):
        timestamp = ingested["ingestion_timestamp"]

        # Convertimos +00:00 → Z (formato que PocketBase acepta sin problemas)
        timestamp = timestamp.replace("+00:00", "Z")

        return {
            "sensor": sensor_id,
            "time": timestamp,
            "value": float(ingested["temp_c"])
        }

    def _flush(self):
        if not self.buffer:
            return

        batch = self.buffer[:]
        self.buffer.clear()

        print(flush=True)
        print(f"FLUSHING {len(batch)} RECORDS", flush=True)
        print(flush=True)

        payload = {
            "requests": [
                {
                    "method": "POST",
                    "url": "/api/collections/readings/records",
                    "body": record
                }
                for record in batch
            ]
        }

        print(flush=True)
        print("BATCH PAYLOAD:", payload, flush=True)
        print(flush=True)

        try:
            response = self.pb.post(
                "/api/batch",
                payload
            )

            logger.info("Flushed batch of %d readings", len(batch))

        except Exception as e:
            logger.error("Batch insert failed: %s", e)

# Crear una única instancia compartida
batch_writer = BatchWriter()

