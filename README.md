# slack-gateway

Slack Socket Mode receiver that decouples Socket Mode from worker Deployments. Receives events from Slack and publishes them to Redis Pub/Sub for workers to consume.

## Architecture

```
Slack (Socket Mode) → gateway pod → Redis Pub/Sub → worker pods
```

One gateway pod holds the Socket Mode connection. Workers subscribe to `slack:events` channel and process events independently.

## Requirements

- `SLACK_APP_TOKEN` env var (required)
- `REDIS_URL` env var (optional, defaults to `redis://localhost:6379/0`)

## Building

```bash
docker build -t slack-gateway:latest .
```

## Running Locally

```bash
export SLACK_APP_TOKEN=xapp-...
export REDIS_URL=redis://localhost:6379/0
go run main.go
```

## Prometheus Metrics

Metrics are exposed on `:9090/metrics`:

- `gateway_slack_reconnects_total` — Socket Mode reconnects, labeled by reason
- `gateway_slack_events_received_total` — Events received from Slack, labeled by event_type
- `gateway_redis_enqueue_latency_seconds` — Latency of publishing to Redis
- `gateway_redis_enqueue_errors_total` — Redis publish errors
- `gateway_uptime_seconds` — Uptime since container start
- `gateway_socket_mode_connected` — Connection status (1/0)

## Graceful Shutdown

On SIGTERM, gateway:
1. Sets Socket Mode connection to disconnected
2. Waits up to 5 seconds for pending acks to Slack
3. Closes Redis connection
4. Exits (k8s restarts pod on RollingUpdate)

Workers automatically rejoin the Redis subscription on restart.

## Deployment

See `/admin/apps/gateway/` in rancher-admin for Kubernetes manifests.
