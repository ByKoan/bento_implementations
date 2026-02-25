import os
import json
import logging

logger = logging.getLogger(__name__)


class DiskQueue:

    '''        
        A simple disk-based queue implementation that allows us to store records in a file on disk. 
        It provides methods to append records, load all records, count the number of records, rewrite the file with a new set of records, and clear the file. 
        This is useful for our batch writer to have a persistent storage of the messages that need to be sent to PocketBase, allowing us to handle retries and ensure no data is lost in case of failures.
    '''

    def __init__(self, file_path: str):
        self.file_path = file_path
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

    # ===============================
    # APPEND
    # ===============================

    def append(self, records):
        with open(self.file_path, "a") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

    # ===============================
    # LOAD
    # ===============================

    def load_all(self):
        if not os.path.exists(self.file_path):
            return []

        with open(self.file_path, "r") as f:
            return [
                json.loads(line.strip())
                for line in f
                if line.strip()
            ]

    # ===============================
    # COUNT
    # ===============================

    def count(self):
        if not os.path.exists(self.file_path):
            return 0

        with open(self.file_path, "r") as f:
            return sum(1 for line in f if line.strip())

    # ===============================
    # REWRITE
    # ===============================

    def rewrite(self, records):
        with open(self.file_path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

    # ===============================
    # CLEAR
    # ===============================

    def clear(self):
        open(self.file_path, "w").close()