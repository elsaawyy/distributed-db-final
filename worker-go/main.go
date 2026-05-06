package main

import (
	"log"
	"net/http"
	"os"

	"github.com/elsaawyy/distributed-db/worker-go/database"
	"github.com/elsaawyy/distributed-db/worker-go/handlers"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[WORKER-GO] ")

	// MySQL Configuration
	mysqlHost := getEnv("MYSQL_HOST", "localhost")
	mysqlPort := getEnv("MYSQL_PORT", "3309")
	mysqlUser := getEnv("MYSQL_USER", "root")
	mysqlPass := getEnv("MYSQL_PASS", "")
	mysqlDB := getEnv("MYSQL_DATABASE", "worker_go_db")

	// Connect to MySQL
	db, err := database.NewMySQLDB(mysqlHost, mysqlPort, mysqlUser, mysqlPass, mysqlDB)
	if err != nil {
		log.Fatal("Failed to connect to MySQL:", err)
	}
	defer db.Close()

	// Initialize database schema
	if err := db.InitSchema(); err != nil {
		log.Fatal("Failed to initialize schema:", err)
	}

	// HTTP Handler
	h := handlers.NewHandler(db)

	// Register routes
	mux := http.NewServeMux()

	// Replication endpoint (called by Master)
	mux.HandleFunc("/replicate", h.Replicate)

	// Read endpoint (fault-tolerant reads)
	mux.HandleFunc("/select", h.Select)

	// Special task: Analytics
	mux.HandleFunc("/analytics", h.Analytics)

	// Health & Status
	mux.HandleFunc("/health", h.Health)
	mux.HandleFunc("/status", h.Status)

	port := getEnv("WORKER_GO_PORT", "8081")
	addr := "0.0.0.0:" + port

	log.Printf("Go Worker node starting on %s", addr)
	log.Printf("MySQL database: %s", mysqlDB)

	if err := http.ListenAndServe(addr, loggingMiddleware(mux)); err != nil {
		log.Fatalf("Go Worker failed to start: %v", err)
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("→ %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
