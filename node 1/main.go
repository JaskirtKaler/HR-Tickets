package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// TicketRequest matching payload from Node 0 client
type TicketRequest struct {
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

// ConcurrencyLimiter controls peak simultaneous outbound worker requests to Node 2
type ConcurrencyLimiter struct {
	sem          chan struct{}
	activeCount  int64
	totalHandled int64
}

func NewConcurrencyLimiter(maxConcurrent int) *ConcurrencyLimiter {
	return &ConcurrencyLimiter{
		sem: make(chan struct{}, maxConcurrent),
	}
}

func (cl *ConcurrencyLimiter) Acquire(ctx context.Context) error {
	select {
	case cl.sem <- struct{}{}:
		atomic.AddInt64(&cl.activeCount, 1)
		atomic.AddInt64(&cl.totalHandled, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cl *ConcurrencyLimiter) Release() {
	<-cl.sem
	atomic.AddInt64(&cl.activeCount, -1)
}

func (cl *ConcurrencyLimiter) Active() int64 {
	return atomic.LoadInt64(&cl.activeCount)
}

func (cl *ConcurrencyLimiter) Total() int64 {
	return atomic.LoadInt64(&cl.totalHandled)
}

// RedisLockManager manages distributed locks in Redis with in-memory fallback
type RedisLockManager struct {
	client      *redis.Client
	isAvailable bool
	mu          sync.Mutex
	memLocks    map[string]*sync.Mutex
}

func NewRedisLockManager(redisAddr string) *RedisLockManager {
	rdb := redis.NewClient(&redis.Options{
		Addr:        redisAddr,
		DialTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	available := false
	if err := rdb.Ping(ctx).Err(); err == nil {
		available = true
		log.Printf("[NODE 1] Connected to Redis at %s", redisAddr)
	} else {
		log.Printf("[NODE 1] Redis unavailable at %s (%v). Using in-memory distributed lock fallback.", redisAddr, err)
	}

	return &RedisLockManager{
		client:      rdb,
		isAvailable: available,
		memLocks:    make(map[string]*sync.Mutex),
	}
}

func (rlm *RedisLockManager) AcquireLock(ctx context.Context, lockKey string, lockValue string, ttl time.Duration) (bool, error) {
	if rlm.isAvailable {
		ok, err := rlm.client.SetNX(ctx, "lock:"+lockKey, lockValue, ttl).Result()
		if err != nil {
			log.Printf("[NODE 1] Redis lock acquisition error for %s: %v", lockKey, err)
			return false, err
		}
		return ok, nil
	}

	// Memory fallback
	rlm.mu.Lock()
	m, exists := rlm.memLocks[lockKey]
	if !exists {
		m = &sync.Mutex{}
		rlm.memLocks[lockKey] = m
	}
	rlm.mu.Unlock()

	m.Lock()
	return true, nil
}

func (rlm *RedisLockManager) ReleaseLock(ctx context.Context, lockKey string, lockValue string) {
	if rlm.isAvailable {
		luaScript := `
			if redis.call("get", KEYS[1]) == ARGV[1] then
				return redis.call("del", KEYS[1])
			else
				return 0
			end
		`
		rlm.client.Eval(ctx, luaScript, []string{"lock:" + lockKey}, lockValue)
		return
	}

	// Memory fallback
	rlm.mu.Lock()
	m, exists := rlm.memLocks[lockKey]
	rlm.mu.Unlock()

	if exists && m != nil {
		m.Unlock()
	}
}

// RateLimiter enforces rate limits per client IP or Employee ID
type RateLimiter struct {
	lockMgr     *RedisLockManager
	maxRequests int
	window      time.Duration
	mu          sync.Mutex
	memHits     map[string][]time.Time
}

func NewRateLimiter(lockMgr *RedisLockManager, maxReqs int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		lockMgr:     lockMgr,
		maxRequests: maxReqs,
		window:      window,
		memHits:     make(map[string][]time.Time),
	}
}

func (rl *RateLimiter) Allow(ctx context.Context, clientKey string) bool {
	if rl.lockMgr.isAvailable {
		redisKey := "ratelimit:" + clientKey
		count, err := rl.lockMgr.client.Incr(ctx, redisKey).Result()
		if err == nil {
			if count == 1 {
				rl.lockMgr.client.Expire(ctx, redisKey, rl.window)
			}
			return count <= int64(rl.maxRequests)
		}
	}

	// Memory fallback
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)

	var validHits []time.Time
	for _, t := range rl.memHits[clientKey] {
		if t.After(cutoff) {
			validHits = append(validHits, t)
		}
	}

	if len(validHits) >= rl.maxRequests {
		rl.memHits[clientKey] = validHits
		return false
	}

	rl.memHits[clientKey] = append(validHits, now)
	return true
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", ":8080")
	node2URL := getEnv("NODE2_URL", "http://localhost:8081/api/process") // second server
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	maxConcurrencyStr := getEnv("MAX_CONCURRENCY", "10") // max limit of requests
	maxConcurrency, _ := strconv.Atoi(maxConcurrencyStr)
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}

	limiter := NewConcurrencyLimiter(maxConcurrency)
	lockMgr := NewRedisLockManager(redisAddr)
	rateLimiter := NewRateLimiter(lockMgr, 20, 10*time.Second) // 20 reqs per 10s

	http.HandleFunc("/api/chat", func(w http.ResponseWriter, r *http.Request) {
		// Enforce CORS
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Employee-ID")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		// Read body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var ticket TicketRequest
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &ticket); err != nil {
				http.Error(w, `{"error":"Invalid JSON payload"}`, http.StatusBadRequest)
				return
			}
		}

		// Rate Limiting Check (by Employee ID or Remote IP)
		clientIdentifier := ticket.EmployeeID
		if clientIdentifier == "" {
			clientIdentifier = r.RemoteAddr
		}

		ctxTimeout, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		if !rateLimiter.Allow(ctxTimeout, clientIdentifier) {
			log.Printf("[NODE 1] Rate limit exceeded for client %s", clientIdentifier)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"Rate limit exceeded. Too many requests. Please slow down."}`))
			return
		}

		// System Concurrency Control (Acquire Worker Slot)
		if err := limiter.Acquire(ctxTimeout); err != nil {
			log.Printf("[NODE 1] Concurrency capacity exceeded for request from %s", clientIdentifier)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"System concurrency limit reached. Please try again shortly."}`))
			return
		}
		defer limiter.Release()

		// Distributed Lock Management (if ticket_id or title is provided)
		lockID := uuid.New().String()
		lockKey := ticket.TicketID
		if lockKey == "" && ticket.Title != "" {
			lockKey = "title:" + ticket.Title
		}
		if lockKey == "" {
			lockKey = "emp:" + clientIdentifier
		}

		acquired, err := lockMgr.AcquireLock(ctxTimeout, lockKey, lockID, 10*time.Second)
		if err != nil || !acquired {
			log.Printf("[NODE 1] Failed to acquire Redis lock for key %s (busy)", lockKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error":"Resource is currently locked by another concurrent operation. Please retry."}`))
			return
		}
		defer lockMgr.ReleaseLock(context.Background(), lockKey, lockID)

		// Forwarding / Proxying Request to Node 2 Server
		log.Printf("[NODE 1] Forwarding request to Node 2 (%s) [Active Workers: %d]", node2URL, limiter.Active())

		proxyReq, err := http.NewRequestWithContext(ctxTimeout, http.MethodPost, node2URL, bytes.NewBuffer(bodyBytes))
		if err != nil {
			http.Error(w, `{"error":"Failed to form backend request"}`, http.StatusInternalServerError)
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 5 * time.Second}
		proxyResp, err := client.Do(proxyReq)
		if err != nil {
			log.Printf("[NODE 1] Node 2 communication error: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(fmt.Sprintf(`{"error":"Node 2 HTTP Server unreachable", "details":%q}`, err.Error())))
			return
		}
		defer proxyResp.Body.Close()

		respBody, err := io.ReadAll(proxyResp.Body)
		if err != nil {
			http.Error(w, `{"error":"Failed to read Node 2 response"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(proxyResp.StatusCode)
		w.Write(respBody)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		statusMap := map[string]interface{}{
			"node":               "node 1 (Manager Gateway)",
			"status":             "healthy",
			"active_concurrency": limiter.Active(),
			"total_requests":     limiter.Total(),
			"redis_available":    lockMgr.isAvailable,
			"target_node2_url":   node2URL,
		}
		json.NewEncoder(w).Encode(statusMap)
	})

	log.Printf("[NODE 1] Manager Gateway Server starting on port %s", port)
	log.Printf("[NODE 1] Concurrency Limit: %d max active workers | Node 2 Target: %s", maxConcurrency, node2URL)

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("[NODE 1] Server failure: %v", err)
	}
}
