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

BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES"))
BASE_DELAY = float(os.getenv("BASE_DELAY"))
MAX_DELAY = float(os.getenv("MAX_DELAY"))

QUEUE_FILE = os.getenv("QUEUE_FILE")


class BatchWriter:

    '''
    The BatchWriter class is responsible for managing the buffering and sending of records to PocketBase.
    It maintains an in-memory buffer of records and a disk-based queue for persistence.
    It has a background thread that periodically flushes the buffer to PocketBase, and another thread
    that retries sending records from the disk queue in case of failures.
    It also handles retries with exponential backoff and sends failed records to an error MQTT topic if they exceed the maximum number of retries.'''

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

        # Buffer thread
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        # Disk retry thread
        self.disk_thread = threading.Thread(target=self._disk_retry_loop, daemon=True)
        self.disk_thread.start()

    # ===============================
    # PUBLIC
    # ===============================
    def add(self, ingested: dict, device_id: str):

        '''
        Add a new record to the buffer.
        If the buffer is currently being processed for disk retries,
        we append directly to the disk queue to avoid conflicts.
        '''

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

        '''
        Build the record in the format required by PocketBase,
        converting the ingestion timestamp to ISO format and rounding the temperature to 2 decimals.
        '''
        
        dt = datetime.fromisoformat(ingested["ingestion_timestamp"].replace("Z", "+00:00"))
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
        return {"sensor": sensor_id, "time": dt.isoformat().replace("+00:00", "Z"), "value": float(ingested["temp_c"])}

    # ===============================
    # BUFFER LOOP
    # ===============================
    def _run(self):

        '''
        Background thread that runs in a loop, sleeping for a defined interval and then checking if the database is alive.
        If the database is down, it saves the current buffer to disk and clears it.
        If the database is alive, it flushes batches of records from the buffer to PocketBase until the buffer is empty.
        '''

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

        '''
        Background thread that runs in a loop, checking for records in the disk queue and trying to resend them to PocketBase.
        It uses a lock to coordinate with the buffer thread and avoid conflicts when accessing the disk queue
        If the database is down, it waits and retries with exponential backoff.
        If a batch of records fails to send after the maximum number of retries, it sends each
        record to the error MQTT topic and removes them from the disk queue to avoid blocking other records.
        '''

        while self.running:
            with self.lock: 
                # We load the disk registers for processing, and mark that we are processing the disk so that the add method knows to write directly to the disk instead of the buffer.
                disk_records = self.disk.load_all()
                self.processing_disk = bool(disk_records)

            if disk_records:
                # We process the disk records in batches, and for each batch we implement a retry mechanism with exponential backoff in case of failures.
                total_records = len(disk_records)
                start_index = 0
                while start_index < total_records and self.running:
                    batch = disk_records[start_index:start_index + BATCH_SIZE]
                    attempt = 0
                    while attempt < MAX_RETRIES and self.running:
                        # Before trying to send the batch, we check if the database is alive. If it's not, we wait with exponential backoff and retry until it comes back up.
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
                                # Reload all record that were left out of the current batch avoiding conflicts with new records
                                all_disk = self.disk.load_all()
                                # Rewrite from disk without the records sent 
                                self.disk.rewrite([r for r in all_disk if r not in batch] + remaining)

                            start_index += len(batch) 
                            break
                        except Exception as e:

                            '''
                            If the batch fails to send, we log the error and retry with exponential backoff until we reach the maximun number of retries
                            If we reach the maximum number of retries, the record will be sent to the errors topic
                            ''' 

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
                # After processing the disk records, we mark that we are no longer processing the disk so that new records can be added to the buffer instead of the disk.
                self.processing_disk = False
            time.sleep(FLUSH_INTERVAL)

    # ===============================
    # HEALTH CHECK
    # ===============================
    def _is_db_alive(self):

        '''Check if the PocketBase database is alive by making a simple GET request to the health endpoint.'''
        
        try:
            response = self.pb.get("/api/health")
            return response.status_code == 200
        except Exception:
            return False

    # ===============================
    # ERROR MQTT
    # ===============================
    def _send_to_error_topic(self, record, reason):

        '''Send a record to the error MQTT topic with a reason for the failure.'''

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

        '''
            Send a batch of records to PocketBase with retries and exponential backoff in case of failures.
            If the batch fails to send after the maximum number of retries, it raises an exception to be handled by the caller.
        '''

        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                # We send the batch to PocketBase using the batch endpoint, and if it fails with a server error (5xx) we raise an exception to trigger the retry mechanism
                payload = {"requests": [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": r} for r in batch]}
                response = self.pb.post("/api/batch", payload)

                # Server error (5xx)
                if response.status_code >= 500:
                    raise Exception(f"Server error {response.status_code}")

                # Succesfully sent (200)
                successfully_sent = []
                if response.status_code == 200:
                    results = response.json()
                    for idx, item in enumerate(results):
                        if item.get("status") == 200:
                            successfully_sent.append(batch[idx])
                        else:
                            self._send_to_error_topic(batch[idx], item)
                
                # Bad request error (4xx)
                elif response.status_code == 400:
                    for record in batch:
                        single_payload = {"requests": [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": record}]}
                        r = self.pb.post("/api/batch", single_payload)
                        '''
                        If the single record fails with a 4xx error, we send it to the errors topic with the error message
                        If it fails with a 5xx error, we raise an exception to trigger the retry mechanism for the whole batch in the next attempt
                        '''
                        if r.status_code == 200:
                            successfully_sent.append(record)
                        else:
                            self._send_to_error_topic(record, r.text)
                return successfully_sent
            except Exception:
                '''
                If there is an exception (like a network error or a server error), we log the error and retry with exponential backoff until we reach the maximum number of retries
                If we reach the maximum number of retries, we raise the exception to be handled by the caller (which will send the records to the error topic)
                '''
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

        '''Flush a batch of records to PocketBase, handling retries and errors.'''

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

        '''
            Manually flush the records in the disk queue to PocketBase. This can be called periodically or on shutdown to ensure that all pending records are sent.
            It loads all records from the disk, tries to send them, and if some fail, it rewrites the disk with the remaining records.
        '''

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


batch_writer = BatchWriter() # Global instance of the batch writer that can be used by the MQTT listener and other components of the application.