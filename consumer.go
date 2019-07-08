package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "encoding/json"
    kafka "github.com/segmentio/kafka-go"
    client "github.com/influxdata/influxdb1-client/v2"

)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 50, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  100 * time.Millisecond,
	})
}

func main() {
    kafkaReader := getKafkaReader("192.168.1.106:9092", "sensors", "group1")
    defer kafkaReader.Close()
    httpClient, err := client.NewHTTPClient(client.HTTPConfig{
        Addr: "http://localhost:8086",
        Username: "sensor",
        Password: "blabla",
    })
    defer httpClient.Close()
    if err != nil {
       fmt.Println("Error: ", err.Error())
    }

	fmt.Println("start consuming ... !!")
	for {
                var result map[string]interface{}
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  "sensors",
			Precision: "s",
		})
                json.Unmarshal([]byte(m.Value),  &result)
		tags := map[string]string {"sensor": result["Device_ID"].(string)}
		timestamp := int64(result["timestamp"].(float64))
		fields := map[string]interface{}{
			"temp": result["temp"].(float64),
			"hum": result["hum"].(float64),
			"bat": result["bat"].(float64),
			"lum": result["lum"].(float64),
			"x": result["x"].(float64),
			"y": result["y"].(float64),
			"z": result["z"].(float64),
			"db": result["db"].(float64),
			"button": int(result["button"].(float64)),
			"timestamp": timestamp,
		}
		pt, err := client.NewPoint(result["Device_ID"].(string), tags, fields, time.Unix(timestamp, 0))
                if err != nil {
                    fmt.Println("Error: ", err.Error())
                }
                bp.AddPoint(pt)
		httpClient.Write(bp)
		fmt.Printf("message: %v %v\n", result["button"], fields)

	}
}

