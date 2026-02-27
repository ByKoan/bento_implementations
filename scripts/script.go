package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

const (
	baseURL    = "http://127.0.0.1:8090/api/collections"
	adminToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb2xsZWN0aW9uSWQiOiJwYmNfMzE0MjYzNTgyMyIsImV4cCI6MTc3MjI3NTAzNSwiaWQiOiJidGg5bXMwMXk5YXc3ZWciLCJyZWZyZXNoYWJsZSI6dHJ1ZSwidHlwZSI6ImF1dGgifQ.Z3gQBmHKUa0bDry-vxJ2FPbsLA7-0PZj1OrDGX-GKz8" // Reemplaza con un token admin válido
	emailAdmin = "admin@example.com"     // User mail that will be created
	password   = "Admin123!"             // User password
)

// Func that create a record in PocketBase
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

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		log.Fatalf("Error creando %s: %s\n%s", collection, resp.Status, string(bodyBytes))
	}

	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)
	return res["id"].(string)
}

// Func to find the user by mail
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

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
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
	timestamp := time.Now().Unix()

	// Create usuario admin
	userID := getUserIDByEmail(emailAdmin)
	if userID == "" {
		userID = createRecord("users", map[string]interface{}{
			"email":           emailAdmin,
			"password":        password,
			"passwordConfirm": password,
			"name":            "Admin",
			"emailVisibility": true,
		})
		fmt.Println("Usuario creado con ID:", userID)
	} else {
		fmt.Println("Usuario ya existe con ID:", userID)
	}

	// Create locations
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
			"point": map[string]float64{
				"lat": loc.lat,
				"lng": loc.lng,
			},
		})
		locIDs = append(locIDs, id)
	}

	// Create devices
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

	// Create devices_locations
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

	// Create sensor_contexts
	sensorContextID := createRecord("sensor_contexts", map[string]interface{}{
		"context": "data",
	})

	// Create sensor_types
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

	// Create sensors
	for _, agvID := range agvIDs {
		for _, stID := range sensorTypeIDs {
			createRecord("sensors", map[string]interface{}{
				"device":      agvID,
				"sensor_type": stID,
			})
		}
	}

	fmt.Println("Inicialización completa: usuario, AGVs, localizaciones, device_locations, sensor_context, sensor_types y sensors creados.")
}