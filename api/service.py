import bentoml
import threading
from mqtt.listener import start
from core.batch_writer import BatchWriter

@bentoml.service(workers=1)
class MQTTService:

    def __init__(self):
        self.batch_writer = BatchWriter()
        thread = threading.Thread(target=start, daemon=True)
        thread.start()
