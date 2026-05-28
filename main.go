package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

var (
	redisClient *redis.Client

	// Prometheus metrics
	socketModeReconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_slack_reconnects_total",
			Help: "Total number of Socket Mode reconnects",
		},
		[]string{"reason"},
	)

	slackEventsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_slack_events_received_total",
			Help: "Total number of Slack events received",
		},
		[]string{"event_type"},
	)

	redisEnqueueLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_redis_enqueue_latency_seconds",
			Help:    "Latency of enqueuing events to Redis",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)

	redisEnqueueErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_redis_enqueue_errors_total",
			Help: "Total number of Redis enqueue errors",
		},
		[]string{"error_reason"},
	)

	gatewayUptime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_uptime_seconds",
			Help: "Gateway uptime in seconds",
		},
	)

	socketModeConnected = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_socket_mode_connected",
			Help: "Socket Mode connection status (1=connected, 0=disconnected)",
		},
	)
)

func init() {
	prometheus.MustRegister(
		socketModeReconnects,
		slackEventsReceived,
		redisEnqueueLatency,
		redisEnqueueErrors,
		gatewayUptime,
		socketModeConnected,
	)
}

func main() {
	appToken := os.Getenv("SLACK_APP_TOKEN")
	if appToken == "" {
		log.Fatal("SLACK_APP_TOKEN not set")
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	// Connect to Redis
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	redisClient = redis.NewClient(opt)
	ctx := context.Background()

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")

	// Initialize Slack client
	api := slack.New(appToken, slack.OptionLog(log.New(os.Stdout, "api: ", log.Lshortfile)))
	client := socketmode.New(api,
		socketmode.OptionDebug(true),
		socketmode.OptionLog(log.New(os.Stdout, "socketmode: ", log.Lshortfile)),
	)

	// Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Starting Prometheus metrics server on :9090")
		if err := http.ListenAndServe(":9090", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Start uptime ticker
	startTime := time.Now()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			gatewayUptime.Set(time.Since(startTime).Seconds())
		}
	}()

	// Track connection status
	var connMutex sync.Mutex
	var isConnected bool

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal, draining...")
		connMutex.Lock()
		isConnected = false
		connMutex.Unlock()
		socketModeConnected.Set(0)

		// Give pending events 5 seconds to ack
		drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Close Socket Mode (this will cause the handler loop to exit)
		client.Close()

		// Wait for graceful shutdown
		select {
		case <-drainCtx.Done():
			log.Println("Drain timeout, exiting")
		}

		redisClient.Close()
		os.Exit(0)
	}()

	// Socket Mode event loop
	go func() {
		for evt := range client.Events {
			switch evt.Type {
			case socketmode.EventTypeConnecting:
				log.Println("Connecting to Slack...")
			case socketmode.EventTypeConnected:
				log.Println("Connected to Slack")
				connMutex.Lock()
				isConnected = true
				connMutex.Unlock()
				socketModeConnected.Set(1)

			case socketmode.EventTypeConnectionError:
				log.Printf("Connection error: %v", evt.Error)
				socketModeReconnects.WithLabelValues("connection_error").Inc()
				connMutex.Lock()
				isConnected = false
				connMutex.Unlock()
				socketModeConnected.Set(0)

			case socketmode.EventTypeDisconnected:
				log.Println("Disconnected from Slack")
				connMutex.Lock()
				isConnected = false
				connMutex.Unlock()
				socketModeConnected.Set(0)

			case socketmode.EventTypeEventsAPI:
				go handleEventsAPI(client, evt, ctx)

			case socketmode.EventTypeSlashCommands:
				// Not handling slash commands in MVP; just ack
				client.Ack(*evt.Request)

			case socketmode.EventTypeInteractive:
				// Not handling interactive events in MVP; just ack
				client.Ack(*evt.Request)

			case socketmode.EventTypeHello:
				log.Println("Received hello event")
			}
		}
		log.Println("Socket Mode event loop exited")
	}()

	// Start Socket Mode
	log.Println("Starting Socket Mode...")
	err = client.Run()
	if err != nil {
		log.Fatalf("Socket Mode error: %v", err)
	}
}

func handleEventsAPI(client *socketmode.Client, evt socketmode.Event, ctx context.Context) {
	eventsAPI := evt.Data.(socketmode.EventsAPIEvent)

	// Ack immediately to Slack
	client.Ack(*evt.Request)

	// Extract event type for metrics
	eventType := "unknown"
	if eventsAPI.InnerEvent != nil {
		eventType = eventsAPI.InnerEvent.Type
	}
	slackEventsReceived.WithLabelValues(eventType).Inc()

	// Serialize event to JSON
	eventJSON, err := json.Marshal(eventsAPI)
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		redisEnqueueErrors.WithLabelValues("marshal_error").Inc()
		return
	}

	// Enqueue to Redis Pub/Sub
	start := time.Now()
	err = redisClient.Publish(ctx, "slack:events", string(eventJSON)).Err()
	latency := time.Since(start).Seconds()
	redisEnqueueLatency.WithLabelValues("slack:events").Observe(latency)

	if err != nil {
		log.Printf("Failed to publish to Redis: %v", err)
		redisEnqueueErrors.WithLabelValues("publish_error").Inc()
		return
	}

	log.Printf("Published event (type=%s, latency=%.3fs) to Redis", eventType, latency)
}
