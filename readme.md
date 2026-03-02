# Bento integration in IoT Template

## About

- ***A project that implements bento with data that comes from an external output `(MQTT)` for trasnform it to simplified data and uploaded to a IoT Template with a `PocketBase` database***

## Project Structure

`-> \api`

`-> \core`

'-> \data`

`-> \mqtt`

`-> \scripts`

`-> \tests`

`.env`

`.env.example`

`Dockerfile`

`docker-compose.yml`

`entrypoint.sh`

`readme.md`

`requirements.txt`

`restart-docker.bat`

`topic_errors.bat`

## Docker instalation

- ***To run the project you need Docker. Run this on your console:***

```bash
docker compose build --no-cache
docker compose up
```

- ***If youre in windows just run restart-docker.bat***

## Example of use:

**See the readme located in scripts folder**

- ***To use this project first run the Docker instalation, you will see the first test passed. now you need to run the Pocketbase db. Run the `send_random_mqtt.py` to see how readings records are created.***
```bash
go run main.go serve
python send_random_mqtt.py
```
- ***To make a simulation run `obtener_token.py` to obtain the token of your _superuser. Paste it in the config variable of Script.go at the top of the file and run the script with:***
```bash
go run Script.go
```

## Principal classes:

`-> \api\service.py` 

- ***Initialize the program: Service class that initializes the MQTT listener and the batch writer. The MQTT listener will receive messages from the devices, enrich them with additional information (like the device_id and a timestamp) and send them to the batch writer, wich will handle the logic of sending the record to PocketBase, handling retries in case of failures, and sending failed record to an error topic in MQTT if they fail after the maximum number of retries.***

`-> \core\batch_writer.py`

- ***The BatchWriter class is responsible for managing the buffering and sending of records to PocketBase. It maintains an in-memory buffer of records and a disk-based queue for persistence. It has a background thread that periodically flushes the buffer to PocketBase, and another thread that retries sending records from the disk queue in case of failures. It also handles retries with exponential backoff and sends failed records to an error MQTT topic if they exceed the maximum number of retries.***

`-> \core\pocketbase_client.py`

- ***A simple client to interact with the PocketBase API, handling authentication and requests. It includes a method to authenticate and obtain a token, a method to make POST requests that automatically re-authenticates if the token is expired, and a method to make GET requests.***

`-> \core\disk_queue.py`

- ***A simple disk-based queue implementation that allows us to store records in a file on disk. It provides methods to append records, load all records, count the number of records, rewrite the file with a new set of records, and clear the file. This is useful for our batch writer to have a persistent storage of the messages that need to be sent to PocketBase, allowing us to handle retries and ensure no data is lost in case of failures.***

`-> \mqtt\listener.py`

- ***MQTT Listener module that connects to the MQTT broker, subscribes to the topic and listens for incoming messages from the devices.
When a message is received, it is parsed, enriched and sent to the batch writer for further processing and storage in the database. If there is an error in the payload or topic, the original message is sent to the errors topic with a reason for the failure.***

- ***With topic_errors.bat you can connect to the topic to listen when a packet fails in the process to upload to db***