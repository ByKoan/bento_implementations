# Bento integration in IoT Template

## Project Structure

    -> `\app` (Main files for the project)

    -> `\mqtt` (Files for mqtt listener)
    
    -> `\core` (Files to transform the data)

    -> `\api` (Files for fastapi to the service of MQTT)

    -> `\tests` (Files for make test to the modules and functions)

    -> Dockerfile
    
    -> docker-compose.yml

    -> requirements.txt

## Docker instalation

- ***To run the project in Docker run this:***

```bash
docker compose build --no-cache
docker compose up
```

- ***For sending messages to MQTT container run this:***

```bash
docker exec -it mqtt_broker mosquitto_pub -t sensores/temperatura -m "{\"temperature_f\": 86}"
```
