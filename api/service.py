import bentoml
import threading
from mqtt.listener import start
with bentoml.importing():
    from core.batch_writer import BatchWriter 

'''
We set the number of workers to 1 to ensure that we have a single instance of the MQTT listener and batch writer running
Wich simplifies the handling of the disk queue and retries
If we wanted to scale the service horizontally, we would need to implement a distributed locking mechanism to ensure that only one instance is flushing
the disk queue at a time, and to handle the retries in a distributed way (for example, using a distributed task queue)
'''

@bentoml.service(workers=1) 
class MQTTService:

    '''
     Service class that initializes the MQTT listener and the batch writer. The MQTT listener will receive messages from the devices,
     enrich them with additional information (like the device_id and a timestamp) and send them to the batch writer,
     wich will handle the logic of sending the record to PocketBase, 
     handling retries in case of failures, and sending failed record to an error topic in MQTT if they fail after the maximum number of retries.
    '''

    def __init__(self):
        # Get instance of BatchWriter
        self.batch_writer = BatchWriter()

        # Start the listener on a thread
        thread = threading.Thread(target=start, args=(self.batch_writer,), daemon=True)
        thread.start()