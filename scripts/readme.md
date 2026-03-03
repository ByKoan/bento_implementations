## How to use this project?

- ***First of all you need to get your DB open, afer that you can run the `send_random_mqtt.py` script.***

- ***This script only send data to readings collection with previous data preloaded, before you run it, you need to create the devices, sensors, locations etc. And after all you need to declare the sensors ids in the .env file at the bottom***

### `script.go` (Simulation):

- ***This script is created to simulate an IoT environment, this script automatically create all, users, locations, devices, sensors etc.***

- **How to run it:**

- ***Launch the following command to run the script***

```bash
# To run this script you need to remove the create rule on readings collection to allow all users create record here
go run script.go | docker exec -i bento_service benthos -c /core/benthos.yaml
``` 