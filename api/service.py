import bentoml
import threading
from mqtt.listener import start, mqtt_client
from core.batch_writer import BatchWriter

@bentoml.service(workers=1)
class MQTTService:
    def __init__(self):
        thread = threading.Thread(target=start, daemon=True)
        thread.start()