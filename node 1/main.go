package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type TicketRequest struct {
	TicketID    string `json:"ticket_id"`
	EmployeeID  string `json:"employee_id"`
	Department  string `json:"department"`
	Category    string `json:"category"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Prompt      string `json:"prompt"`
}

func main() {
	// define routes (URL paths -> handler functions)
	http.HandleFunc("/api/chat", func(w http.ResponseWriter, r *http.Request) {
		// Tell the browser CORS is allowed  *--web security---*
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		// Handle browser CORS preflight check
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// create variabel for parsed form
		var ticket TicketRequest

		err := json.NewDecoder(r.Body).Decode(&ticket)
		if err != nil {
			http.Error(w, "Invalid json payload", http.StatusBadRequest) // error 400
			return
		}

		fmt.Printf("recived ticket title %s\n", ticket.Title)
		fmt.Printf("Empylee ID: %s\n", ticket.EmployeeID)

	})

	// server port
	port := ":8080"

	fmt.Printf("Server is running on port %s ....\n", port)

	// start server
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("Server error: ", err) // error with server
	}
}
