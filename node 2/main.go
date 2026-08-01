package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type TicketPayload struct {
	TicketID    string `json:"ticket_id,omitempty"`
	EmployeeID  string `json:"employee_id,omitempty"`
	Department  string `json:"department,omitempty"`
	Category    string `json:"category,omitempty"`
	Status      string `json:"status,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Prompt      string `json:"prompt,omitempty"`
}

type ServiceResponse struct {
	Status      string         `json:"status"`
	Node        string         `json:"processed_by"`
	Timestamp   string         `json:"timestamp"`
	Ticket      *TicketPayload `json:"ticket,omitempty"`
	Answer      string         `json:"answer,omitempty"`
	TicketID    string         `json:"ticket_id,omitempty"`
	Title       string         `json:"title,omitempty"`
	EmployeeID  string         `json:"employee_id,omitempty"`
	Department  string         `json:"department,omitempty"`
	Category    string         `json:"category,omitempty"`
	Description string         `json:"description,omitempty"`
	CreatedAt   string         `json:"created_at,omitempty"`
}

func main() {
	http.HandleFunc("/api/process", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var payload TicketPayload
		err := json.NewDecoder(r.Body).Decode(&payload)
		if err != nil {
			http.Error(w, `{"error":"Invalid JSON payload"}`, http.StatusBadRequest)
			return
		}

		// Simulate minor backend processing time
		time.Sleep(100 * time.Millisecond)

		nowStr := time.Now().Format(time.RFC3339)
		log.Printf("[NODE 2] Processing request - Title: %q, Prompt: %q, EmployeeID: %q", payload.Title, payload.Prompt, payload.EmployeeID)

		resp := ServiceResponse{
			Status:    "success",
			Node:      "Node-2-Worker",
			Timestamp: nowStr,
		}

		// Handle AI Prompt query vs Ticket creation
		if payload.Prompt != "" && payload.Title == "" {
			resp.Answer = fmt.Sprintf("🤖 HR AI Assistant Response (Node 2): Received query %q. Your HR ticket/inquiry has been cataloged and categorized under HR Policy standards.", payload.Prompt)
		} else {
			ticketID := payload.TicketID
			if ticketID == "" {
				ticketID = fmt.Sprintf("TICK-%d", time.Now().UnixNano()%100000)
			}
			resp.TicketID = ticketID
			resp.Title = payload.Title
			resp.EmployeeID = payload.EmployeeID
			resp.Department = payload.Department
			resp.Category = payload.Category
			resp.Description = payload.Description
			resp.CreatedAt = payload.CreatedAt
			resp.Status = "Submitted & Logged"

			resp.Ticket = &TicketPayload{
				TicketID:    ticketID,
				EmployeeID:  payload.EmployeeID,
				Department:  payload.Department,
				Category:    payload.Category,
				Status:      "Submitted & Logged",
				CreatedAt:   payload.CreatedAt,
				Title:       payload.Title,
				Description: payload.Description,
			}
			resp.Answer = fmt.Sprintf("Ticket %s created successfully and assigned to HR queue.", ticketID)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"node":"node 2","status":"healthy"}`))
	})

	port := ":8081"
	log.Printf("[NODE 2] Backend Server running on http://localhost%s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("[NODE 2] Server failure: %v", err)
	}
}
