package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Ticket struct {
	TicketID    string `json:"ticket_id"`
	EmployeeID  string `json:"employee_id"`
	Department  string `json:"department"`
	Category    string `json:"category"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type MetricCounter struct {
	Status200 int64
	Status429 int64
	Status503 int64
	OtherErr  int64
}

func main() {
	targetURL := "http://localhost:8080/api/chat"

	// Find and read tickets.json
	jsonPath := "tickets.json"
	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		jsonPath = filepath.Join("test", "tickets.json")
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		log.Fatalf("[TEST CLIENT] Failed to read tickets JSON file: %v", err)
	}
	// could not parse json
	var tickets []Ticket
	if err := json.Unmarshal(data, &tickets); err != nil {
		log.Fatalf("[TEST CLIENT] Failed to parse JSON: %v", err)
	}

	fmt.Println("==========================================================================")
	fmt.Printf("🚀 Starting Multithreaded Goroutine Stress Test against Node 1 (%s)\n", targetURL)
	fmt.Printf("📦 Loaded %d sample tickets from %s\n", len(tickets), jsonPath)
	fmt.Println("==========================================================================")

	var wg sync.WaitGroup     // counter to wait for all threads
	var metrics MetricCounter // struct to track errors
	startTime := time.Now()

	// Spawn a separate Goroutine for EACH ticket request concurrently!
	for i, t := range tickets { // for each ticket start a go routine
		wg.Add(1) // add to counter

		go func(reqNum int, ticket Ticket) { // go routine starts here
			defer wg.Done()

			payloadBytes, err := json.Marshal(ticket)
			if err != nil {
				atomic.AddInt64(&metrics.OtherErr, 1)
				log.Printf("❌ [#%02d] JSON Marshal Error: %v", reqNum, err)
				return
			}

			reqStart := time.Now()
			req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewBuffer(payloadBytes))
			if err != nil {
				atomic.AddInt64(&metrics.OtherErr, 1)
				log.Printf("❌ [#%02d] Request creation error: %v", reqNum, err)
				return
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			elapsed := time.Since(reqStart)

			if err != nil {
				atomic.AddInt64(&metrics.OtherErr, 1)
				log.Printf("💥 [#%02d] Failed to reach Node 1: %v (Duration: %v)", reqNum, err, elapsed)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			switch resp.StatusCode {
			case http.StatusOK:
				atomic.AddInt64(&metrics.Status200, 1)
				log.Printf("✅ [#%02d] 200 OK | Ticket: %s | Emp: %s | Duration: %v", reqNum, ticket.TicketID, ticket.EmployeeID, elapsed)
			case http.StatusTooManyRequests:
				atomic.AddInt64(&metrics.Status429, 1)
				log.Printf("⚠️ [#%02d] 429 RATE LIMITED | Emp: %s | Msg: %s | Duration: %v", reqNum, ticket.EmployeeID, string(body), elapsed)
			case http.StatusServiceUnavailable:
				atomic.AddInt64(&metrics.Status503, 1)
				log.Printf("🛑 [#%02d] 503 CONCURRENCY EXCEEDED | Msg: %s | Duration: %v", reqNum, string(body), elapsed)
			default:
				atomic.AddInt64(&metrics.OtherErr, 1)
				log.Printf("Status %d | Msg: %s | Duration: %v", reqNum, resp.StatusCode, string(body), elapsed)
			}
		}(i+1, t)
	}

	// Wait for all 20 Goroutines to finish!
	wg.Wait()
	totalDuration := time.Since(startTime)

	fmt.Println("\n==========================================================================")
	fmt.Println("📊 GOROUTINE TEST RESULTS SUMMARY")
	fmt.Println("==========================================================================")
	fmt.Printf("⏱️  Total Test Execution Time: %v\n", totalDuration)
	fmt.Printf("Total Requests Sent:        %d (20 Goroutines in parallel)\n", len(tickets))
	fmt.Printf("Successful (200 OK):       %d\n", metrics.Status200)
	fmt.Printf("Rate Limited (429):        %d\n", metrics.Status429)
	fmt.Printf("Concurrency Throttled(503): %d\n", metrics.Status503)
	fmt.Printf("Errors / Failed:           %d\n", metrics.OtherErr)
	fmt.Println("==========================================================================")
}
