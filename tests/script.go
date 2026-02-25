package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// ----------------------------
// CONFIG
// ----------------------------
const (
	NumAGVs              = 5
	UpdatePeriod         = 1 * time.Second
	MaxBattery           = 100.0
	MinBattery           = 0.0
	BatteryDrainPerStep  = 0.5
	StepsPerPalletUpdate = 10

	PocketBaseURL = "http://127.0.0.1:8090/api/collections"
	AdminToken    = ""
)

// ----------------------------
// TIPOS
// ----------------------------
type MissionState string

const (
	Idle     MissionState = "IDLE"
	Moving   MissionState = "MOVING"
	Charging MissionState = "CHARGING"
	Error    MissionState = "ERROR"
)

type AGV struct {
	ID                   string
	DeviceID             string
	X                    float64
	Y                    float64
	Battery              float64
	Mission              MissionState
	SensorID             string
	StateSensorID        string
	HasPalletSensorID    string
	TargetX              float64
	TargetY              float64
	stepsSinceLastPallet int
	batteryOffset        float64
}

// ----------------------------
// GENERAL FUNCTIONS
// ----------------------------
func createRecord(collection string, payload map[string]interface{}) (string, error) {
	url := fmt.Sprintf("%s/%s/records", PocketBaseURL, collection)
	data, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+AdminToken)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	idStr, ok := result["id"].(string)
	if !ok {
		return "", fmt.Errorf("no id returned for %s, body: %v", collection, result)
	}
	return idStr, nil
}

// ----------------------------
// DATA INITIALIZATION
// ----------------------------
func initializeBaseData() ([]*AGV, map[string][2]float64, []string, error) {
	adminID, _ := createRecord("users", map[string]interface{}{
		"email":           fmt.Sprintf("admin_%d@example.com", time.Now().UnixNano()),
		"password":        "admin123",
		"passwordConfirm": "admin123",
		"emailVisibility": true,
		"name":            "Admin User",
	})
	operatorID, _ := createRecord("users", map[string]interface{}{
		"email":           fmt.Sprintf("operator_%d@example.com", time.Now().UnixNano()),
		"password":        "operator123",
		"passwordConfirm": "operator123",
		"emailVisibility": true,
		"name":            "Operator User",
	})

	locations := make(map[string][2]float64)
	locEntradaID, _ := createRecord("locations", map[string]interface{}{
		"name": "Entrada Planta",
		"point": map[string]interface{}{"x": 10.0, "y": 10.0},
	})
	locCargaID, _ := createRecord("locations", map[string]interface{}{
		"name": "Carga Planta",
		"point": map[string]interface{}{"x": 50.0, "y": 40.0},
	})
	locSalidaID, _ := createRecord("locations", map[string]interface{}{
		"name": "Salida Planta",
		"point": map[string]interface{}{"x": 90.0, "y": 20.0},
	})
	locations[locEntradaID] = [2]float64{10.0, 10.0}
	locations[locCargaID] = [2]float64{50.0, 40.0}
	locations[locSalidaID] = [2]float64{90.0, 20.0}
	locIDs := []string{locEntradaID, locCargaID, locSalidaID}

	ctxDATA, _ := createRecord("sensor_contexts", map[string]interface{}{"context": "DATA"})
	batteryType, _ := createRecord("sensor_types", map[string]interface{}{"sensor_context": ctxDATA, "magnitude": "battery", "unit": "%"})
	// Actualizamos has_pallet para enviar 0/1
	hasPalletType, _ := createRecord("sensor_types", map[string]interface{}{"sensor_context": ctxDATA, "magnitude": "has_pallet", "unit": "0/1"})
	stateType, _ := createRecord("sensor_types", map[string]interface{}{"sensor_context": ctxDATA, "magnitude": "mission_state", "unit": "1/2/3/4"})

	agvs := make([]*AGV, NumAGVs)
	for i := 0; i < NumAGVs; i++ {
		userID := operatorID
		if i%2 == 0 {
			userID = adminID
		}
		devName := fmt.Sprintf("AGV-%d-%d", i+1, time.Now().UnixNano())
		devID, _ := createRecord("devices", map[string]interface{}{"user": userID, "name": devName})

		sensorID, _ := createRecord("sensors", map[string]interface{}{"device": devID, "sensor_type": batteryType})
		hasPalletSensorID, _ := createRecord("sensors", map[string]interface{}{"device": devID, "sensor_type": hasPalletType})
		stateSensorID, _ := createRecord("sensors", map[string]interface{}{"device": devID, "sensor_type": stateType})

		_, _ = createRecord("devices_locations", map[string]interface{}{
			"location": locEntradaID,
			"device":   devID,
			"placed_at": time.Now().Format(time.RFC3339),
		})

		agvs[i] = &AGV{
			ID:                   devName,
			DeviceID:             devID,
			X:                    locations[locEntradaID][0],
			Y:                    locations[locEntradaID][1],
			Battery:              MaxBattery,
			Mission:              Idle,
			SensorID:             sensorID,
			HasPalletSensorID:    hasPalletSensorID,
			StateSensorID:        stateSensorID,
			TargetX:              locations[locCargaID][0],
			TargetY:              locations[locCargaID][1],
			stepsSinceLastPallet: rand.Intn(StepsPerPalletUpdate),
			batteryOffset:        rand.Float64() * BatteryDrainPerStep,
		}
	}
	return agvs, locations, locIDs, nil
}

// ----------------------------
// SIMULATION
// ----------------------------
func (agv *AGV) Update(locations map[string][2]float64, locIDs []string) {
	// CHARGING si batería < 90
	if agv.Battery < 90 {
		agv.Mission = Charging
	}

	// MOVING si estaba Idle
	if agv.Mission == Idle && rand.Float64() < 0.2 {
		agv.Mission = Moving
		agv.TargetX = locations[locIDs[1]][0]
		agv.TargetY = locations[locIDs[1]][1]
	}

	// Movimiento
	if agv.Mission == Moving {
		dx := agv.TargetX - agv.X
		dy := agv.TargetY - agv.Y
		dist := math.Sqrt(dx*dx + dy*dy)
		step := 1.0
		if dist > step {
			agv.X += dx / dist * step
			agv.Y += dy / dist * step
		} else {
			agv.X = agv.TargetX
			agv.Y = agv.TargetY
			agv.Mission = Idle
		}
	}

	// Batería
	if agv.Mission != Charging && agv.Battery > MinBattery {
		agv.Battery -= BatteryDrainPerStep - agv.batteryOffset
		if agv.Battery < MinBattery {
			agv.Battery = MinBattery
			agv.Mission = Error
		}
	} else if agv.Mission == Charging {
		agv.Battery += 1.5
		if agv.Battery >= MaxBattery {
			agv.Battery = MaxBattery
			agv.Mission = Idle
		}
	}
}

// ----------------------------
// SEND DATA
// ----------------------------
func (agv *AGV) SendReading() {
	// Batería
	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.SensorID,
		"time":   time.Now().Format(time.RFC3339),
		"value":  agv.Battery,
	})

	// HasPallet (0/1)
	hasPallet := 0
	agv.stepsSinceLastPallet++
	if agv.stepsSinceLastPallet >= StepsPerPalletUpdate {
		hasPallet = 1
		agv.stepsSinceLastPallet = 0
	}
	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.HasPalletSensorID,
		"time":   time.Now().Format(time.RFC3339),
		"value":  hasPallet,
	})

	// Estado como número
	stateNum := 1
	switch agv.Mission {
	case Idle:
		stateNum = 1
	case Moving:
		stateNum = 2
	case Charging:
		stateNum = 3
	case Error:
		stateNum = 4
	}
	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.StateSensorID,
		"time":   time.Now().Format(time.RFC3339),
		"value":  stateNum,
	})
}

// ----------------------------
// MAIN
// ----------------------------
func main() {
	rand.Seed(time.Now().UnixNano())

	agvs, locations, locIDs, err := initializeBaseData()
	if err != nil {
		fmt.Println("Error inicializando datos:", err)
		return
	}

	var wg sync.WaitGroup
	for _, agv := range agvs {
		wg.Add(1)
		go func(a *AGV) {
			defer wg.Done()
			ticker := time.NewTicker(UpdatePeriod)
			for range ticker.C {
				a.Update(locations, locIDs)
				a.SendReading()
			}
		}(agv)
	}
	wg.Wait()
}