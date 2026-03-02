/*

	TO RUN THIS SIMULATION, FIRST YOU NEED TO OBTAIN YOUR SUPER USER TOKEN RUNNING THE obtener_token.py SCRIPT AND PUT IT IN adminToken VARIABLE

*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

const (
	baseURL       = "http://127.0.0.1:8090/api/collections" // URL for upload the content
	adminToken    = "" // Replaze this with your super user token 
	emailAdmin    = "admin@example.com" // Email for the new user that will be created
	password      = "Admin123!"  // Password for the new user that will be created
	updatePeriod  = 5 * time.Second // reading transmission interval
	batteryMin    = 20              // % minimun battery after charging
	hasPalletTick = 5               // ticks for pallet flag
)

// DataStruct with the content of AGV
type AGV struct {
	ID          string
	Temperature float64
	Battery     float64
	Status      int
	HasPallet   int
	PalletTicks  int
	SensorIDs   map[string]string // "temperature","battery","has_pallet","status"
}

// Reading sended to benthos
// This is in json, because benthos is configured in stdin and we need to send every line in json
type Reading struct {
	Sensor             string  `json:"sensor"`
	Value              float64 `json:"value"`
	HasPallet          int     `json:"has_pallet"`
	Status             int     `json:"status"`
	Time 			   string  `json:"time"`
	TempC              float64 `json:"temp_c"`
}

// Helper function to create new records in pocket base 
func createRecord(collection string, data map[string]interface{}) string {
	url := fmt.Sprintf("%s/%s/records", baseURL, collection)
	jsonData, _ := json.Marshal(data)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		log.Fatalf("Error creando %s: %s\n%s", collection, resp.Status, string(bodyBytes))
	}

	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)
	return res["id"].(string)
}

// Find user by email
func getUserIDByEmail(email string) string {
	url := fmt.Sprintf("%s/users/records?filter=email='%s'", baseURL, email)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)

	items, ok := res["items"].([]interface{})
	if ok && len(items) > 0 {
		user := items[0].(map[string]interface{})
		return user["id"].(string)
	}
	return ""
}

func main() {
	rand.Seed(time.Now().UnixNano())
	timestamp := time.Now().Unix()

	// Create and upload to DataBase new user admin
	userID := getUserIDByEmail(emailAdmin)
	if userID == "" {
		userID = createRecord("users", map[string]interface{}{
			"email":           emailAdmin,
			"password":        password,
			"passwordConfirm": password,
			"name":            "Admin",
			"emailVisibility": true,
		})
		fmt.Fprintln(os.Stderr, "Usuario creado con ID:", userID)
	} else {
		fmt.Fprintln(os.Stderr, "Usuario ya existe con ID:", userID)
	}

	// Create and upload to DataBase new locations
	locations := []struct {
		name string
		lat  float64
		lng  float64
	}{
		{"Zona A", 33.4, 45.2},
		{"Zona B", 10.5, 5.2},
		{"Zona C", -3.3, 8.8},
	}
	locIDs := make([]string, 0, len(locations))
	for _, loc := range locations {
		id := createRecord("locations", map[string]interface{}{
			"name": loc.name,
			"point": map[string]interface{}{
				"lat": loc.lat,
				"lon": loc.lng,
			},
		})
		locIDs = append(locIDs, id)
	}

	// Create and upload to DataBase new devices
	agvs := []string{
		fmt.Sprintf("AGV-01-%d", timestamp),
		fmt.Sprintf("AGV-02-%d", timestamp),
		fmt.Sprintf("AGV-03-%d", timestamp),
		fmt.Sprintf("AGV-04-%d", timestamp),
		fmt.Sprintf("AGV-05-%d", timestamp),
	}
	agvIDs := make([]string, 0, len(agvs))
	for _, name := range agvs {
		id := createRecord("devices", map[string]interface{}{
			"user": userID,
			"name": name,
		})
		agvIDs = append(agvIDs, id)
	}

	// Create and upload to DataBase new devices_locations
	assignments := []struct {
		agvIndex int
		locIndex int
	}{
		{0, 0}, {1, 1}, {2, 2}, {3, 0}, {4, 1},
	}
	for _, a := range assignments {
		createRecord("devices_locations", map[string]interface{}{
			"device":    agvIDs[a.agvIndex],
			"location":  locIDs[a.locIndex],
			"placed_at": time.Now().Format(time.RFC3339),
		})
	}

	// Create and upload to DataBase new sensor_contexts
	sensorContextID := createRecord("sensor_contexts", map[string]interface{}{
		"context": "data", // We only have 1 context right now because we only have 4 sensors per device 
	})

	// Create and upload to DataBase new sensor_types
	sensorTypes := []struct {
		magnitude string
		unit      string
	}{
		{"temperature", "°C"},
		{"battery", "%"},
		{"has_pallet", "0/1"},
		{"status", "0/1/2/3"},
	}
	sensorTypeIDs := make([]string, 0, len(sensorTypes))
	for _, st := range sensorTypes {
		id := createRecord("sensor_types", map[string]interface{}{
			"sensor_context": sensorContextID,
			"magnitude":      st.magnitude,
			"unit":           st.unit,
		})
		sensorTypeIDs = append(sensorTypeIDs, id)
	}

	// Create and upload to DataBase sensors
	sensorsMap := map[string]map[string]string{} // agvID -> sensor_type -> sensorID
	for _, agvID := range agvIDs {
		sensorsMap[agvID] = map[string]string{}
		for i, stID := range sensorTypeIDs {
			sensorID := createRecord("sensors", map[string]interface{}{
				"device":      agvID,
				"sensor_type": stID,
			})
			switch i {
			case 0:
				sensorsMap[agvID]["temperature"] = sensorID
			case 1:
				sensorsMap[agvID]["battery"] = sensorID
			case 2:
				sensorsMap[agvID]["has_pallet"] = sensorID
			case 3:
				sensorsMap[agvID]["status"] = sensorID
			}
		}
	}

	fmt.Fprintln(os.Stderr, "Inicialización completa. Empezando simulación de lecturas...")

	// Real time readings simulation for benthos
	agvObjs := []*AGV{}
	for _, agvID := range agvIDs {
		// Starter values 
		agvObjs = append(agvObjs, &AGV{
			ID:          agvID,
			Temperature: 20 + rand.Float64()*10,
			Battery:     100,
			Status:      1,
			HasPallet:   0,
			PalletTicks: 0,
			SensorIDs:   sensorsMap[agvID],
		})
	}

	ticker := time.NewTicker(updatePeriod)
	defer ticker.Stop()

	for range ticker.C {
		for _, agv := range agvObjs {
			// Random temperature between 15 - 35 
			agv.Temperature += rand.Float64()*2 - 1
			if agv.Temperature < 15 {
				agv.Temperature = 15
			}
			if agv.Temperature > 35 {
				agv.Temperature = 35
			}

			// Battery 
			agv.Battery -= rand.Float64() * 2
			if agv.Battery < 0 {
				agv.Battery = 100
			}

			// Status
			agv.Status = 1
			if rand.Intn(10) < 3 || agv.HasPallet == 1 { // If has_pallet is checked (value = 1) automatically the AGV will be moving (status value = 2)
				agv.Status = 2
			}
			if agv.Battery < batteryMin {
				agv.Status = 3
			}
			if rand.Intn(100) < 2 {
				agv.Status = 4
			}

			// HasPallet
			if agv.PalletTicks == 0 && rand.Intn(5) == 0 {
				agv.HasPallet = 1
				agv.PalletTicks = hasPalletTick
			}
			if agv.PalletTicks > 0 {
				agv.PalletTicks--
				if agv.PalletTicks == 0 {
					agv.HasPallet = 0
				}
			}

			// Generate readings 
			readings := []Reading{
				{Sensor: agv.SensorIDs["temperature"], Value: agv.Temperature, HasPallet: agv.HasPallet, Status: agv.Status, Time: time.Now().UTC().Format(time.RFC3339), TempC: agv.Temperature},
				{Sensor: agv.SensorIDs["battery"], Value: agv.Battery, HasPallet: agv.HasPallet, Status: agv.Status, Time: time.Now().UTC().Format(time.RFC3339), TempC: agv.Temperature},
				{Sensor: agv.SensorIDs["has_pallet"], Value: float64(agv.HasPallet), HasPallet: agv.HasPallet, Status: agv.Status, Time: time.Now().UTC().Format(time.RFC3339), TempC: agv.Temperature},
				{Sensor: agv.SensorIDs["status"], Value: float64(agv.Status), HasPallet: agv.HasPallet, Status: agv.Status, Time: time.Now().UTC().Format(time.RFC3339), TempC: agv.Temperature},
			}

			// Send to stdout for benthos
			for _, r := range readings {
				j, _ := json.Marshal(r)
				fmt.Println(string(j))
			}
		}
	}
}