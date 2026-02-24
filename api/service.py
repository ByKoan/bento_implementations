import bentoml
import threading
from mqtt.listener import start, mqtt_client
from core.batch_writer import BatchWriter


@bentoml.service(workers=1)
class MQTTService:

    def __init__(self):

        self.batch_writer = BatchWriter(mqtt_client=mqtt_client)

        thread = threading.Thread(
            target=start,
            args=(self.batch_writer,)
        )
        thread.start()