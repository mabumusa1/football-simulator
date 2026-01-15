---
layout: default
title: API Reference
nav_order: 4
---

# API Reference

Complete documentation for the Fanfinity API.

## Base URL

| Environment | URL |
|-------------|-----|
| Development | `http://localhost:8080` |
| Production | `https://api.your-domain.com` |

## Authentication

All `/api/*` endpoints require an API key via the `X-API-Key` header.

```bash
curl -H "X-API-Key: your-api-key" https://api.your-domain.com/api/events
```

Public endpoints (`/health`, `/ready`, `/metrics`, `/`) do not require authentication.

---

## Health Endpoints

### GET /health

Simple liveness check. Returns 200 if the service is running.

**Request:**
```bash
curl http://localhost:8080/health
```

**Response (200 OK):**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T14:30:00Z"
}
```

---

### GET /ready

Readiness check that verifies Kafka and ClickHouse connectivity.

**Request:**
```bash
curl http://localhost:8080/ready
```

**Response (200 OK):**
```json
{
  "status": "ready",
  "timestamp": "2024-01-15T14:30:00Z",
  "checks": {
    "clickhouse": "healthy",
    "kafka": "healthy"
  }
}
```

**Response (503 Service Unavailable):**
```json
{
  "status": "not ready",
  "timestamp": "2024-01-15T14:30:00Z",
  "checks": {
    "clickhouse": "unhealthy: connection refused",
    "kafka": "healthy"
  }
}
```

---

## Documentation Endpoints

### GET /

Swagger UI - Interactive API documentation.

### GET /openapi.yaml

OpenAPI 3.0.3 specification file.

### GET /metrics

Prometheus metrics endpoint.

---

## Match Events API

### POST /api/events

Ingest a match event (goal, pass, foul, etc.).

**Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | `application/json` |
| `X-API-Key` | Yes | Your API key |

**Request Body:**

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "matchId": "match-2024-001",
  "eventType": "goal",
  "timestamp": "2024-01-15T14:30:00Z",
  "teamId": 1,
  "playerId": "player-123",
  "metadata": {
    "minute": 45,
    "assistPlayerId": "player-456"
  }
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `eventId` | string (UUID) | Yes | Unique event identifier |
| `matchId` | string | Yes | Match identifier |
| `eventType` | string (enum) | Yes | Type of event |
| `timestamp` | string (RFC3339) | Yes | Event timestamp |
| `teamId` | integer | Yes | Team identifier (1 or 2) |
| `playerId` | string | No | Player identifier |
| `metadata` | object | No | Additional event data |

**Event Types:**

| Type | Description |
|------|-------------|
| `pass` | Completed pass |
| `shot` | Shot attempt |
| `goal` | Goal scored |
| `foul` | Foul committed |
| `yellow_card` | Yellow card shown |
| `red_card` | Red card shown |
| `substitution` | Player substitution |
| `offside` | Offside call |
| `corner` | Corner kick awarded |
| `free_kick` | Free kick awarded |
| `interception` | Ball intercepted |

**Response (202 Accepted):**

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "timestamp": "2024-01-15T14:30:00Z"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "eventId": "550e8400-e29b-41d4-a716-446655440000",
    "matchId": "match-2024-001",
    "eventType": "goal",
    "timestamp": "2024-01-15T14:30:00Z",
    "teamId": 1,
    "playerId": "player-10",
    "metadata": {
      "minute": 45,
      "assistPlayerId": "player-7"
    }
  }'
```

---

## Engagement Events API

### POST /api/engagements

Batch ingest viewer engagement events.

**Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | `application/json` |
| `X-API-Key` | Yes | Your API key |

**Request Body:**

```json
{
  "events": [
    {
      "event_id": "550e8400-e29b-41d4-a716-446655440001",
      "match_id": "match-2024-001",
      "user_id": "user-456",
      "session_id": "session-789",
      "engagement_type": "reaction",
      "engagement_subtype": "emoji_goal",
      "related_game_event_id": "550e8400-e29b-41d4-a716-446655440000",
      "game_minute": 45,
      "device_type": "mobile",
      "platform": "ios",
      "country_code": "US",
      "content": "GOOOAL!",
      "metadata": {},
      "timestamp": "2024-01-15T14:30:01Z"
    }
  ]
}
```

**Event Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `event_id` | string (UUID) | Yes | Unique event identifier |
| `match_id` | string | Yes | Match identifier |
| `user_id` | string | Yes | User identifier |
| `session_id` | string | Yes | Session identifier |
| `engagement_type` | string (enum) | Yes | Type of engagement |
| `engagement_subtype` | string | No | Subtype of engagement |
| `related_game_event_id` | string (UUID) | No | Related match event |
| `game_minute` | integer | Yes | Match minute (0-120) |
| `device_type` | string (enum) | Yes | Device type |
| `platform` | string | No | Platform (ios, android, web) |
| `country_code` | string | No | ISO country code |
| `content` | string | No | User-generated content |
| `metadata` | object | No | Additional data |
| `timestamp` | string (RFC3339) | Yes | Event timestamp |

**Engagement Types and Subtypes:**

| Type | Subtypes |
|------|----------|
| `reaction` | cheer, boo, emoji_goal, emoji_fire, emoji_clap, emoji_cry, emoji_angry, emoji_heart, emoji_laugh, emoji_wow |
| `comment` | match_commentary, player_discussion, team_support, trash_talk, question |
| `video_action` | pause, play, rewind, replay, camera_switch, quality_change, fullscreen |
| `share` | twitter, facebook, whatsapp, instagram, in_app, copy_link |
| `prediction` | score_prediction, next_goal, player_rating, poll_vote, man_of_match |
| `click` | stats_view, player_profile, team_info, lineup, ad_click, merchandise, ticket |
| `session` | join, leave, heartbeat, reconnect |

**Device Types:**

| Type | Description |
|------|-------------|
| `mobile` | Mobile phone |
| `desktop` | Desktop computer |
| `tablet` | Tablet device |
| `tv` | Smart TV or streaming device |
| `unknown` | Unknown device type |

**Response (202 Accepted):**

```json
{
  "accepted": 1,
  "rejected": 0,
  "status": "accepted",
  "timestamp": "2024-01-15T14:30:02Z"
}
```

**Example:**

```bash
curl -X POST http://localhost:8080/api/engagements \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "events": [
      {
        "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "match_id": "match-2024-001",
        "user_id": "user-123",
        "session_id": "sess-456",
        "engagement_type": "reaction",
        "engagement_subtype": "emoji_goal",
        "game_minute": 45,
        "device_type": "mobile",
        "platform": "ios",
        "timestamp": "2024-01-15T14:30:01Z"
      },
      {
        "event_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
        "match_id": "match-2024-001",
        "user_id": "user-789",
        "session_id": "sess-012",
        "engagement_type": "comment",
        "engagement_subtype": "match_commentary",
        "game_minute": 45,
        "device_type": "desktop",
        "content": "What a goal!",
        "timestamp": "2024-01-15T14:30:02Z"
      }
    ]
  }'
```

---

## Metrics API

### GET /api/matches/{matchId}/metrics

Query aggregated metrics for a match.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `matchId` | string | Match identifier |

**Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `X-API-Key` | Yes | Your API key |

**Response (200 OK):**

```json
{
  "matchId": "match-2024-001",
  "totalEvents": 1250,
  "eventsByType": {
    "pass": 800,
    "shot": 150,
    "goal": 2,
    "yellow_card": 3,
    "red_card": 0
  },
  "goals": 2,
  "yellowCards": 3,
  "redCards": 0,
  "firstEventAt": "2024-01-15T14:00:00Z",
  "lastEventAt": "2024-01-15T15:45:00Z",
  "peakMinute": {
    "minute": "2024-01-15T14:45:00Z",
    "eventCount": 42
  },
  "responseTimePercentiles": {
    "p50": 15.5,
    "p95": 45.2,
    "p99": 78.9
  }
}
```

**Example:**

```bash
curl http://localhost:8080/api/matches/match-2024-001/metrics \
  -H "X-API-Key: your-api-key"
```

---

## Error Responses

All errors follow a consistent format:

```json
{
  "error": "Bad Request",
  "message": "invalid JSON body",
  "field": "eventType"
}
```

**Status Codes:**

| Code | Description |
|------|-------------|
| 400 | Bad Request - Invalid input data |
| 401 | Unauthorized - Missing or invalid API key |
| 404 | Not Found - Resource not found |
| 500 | Internal Server Error - Server error |
| 503 | Service Unavailable - Dependencies unhealthy |

---

## Rate Limiting

Currently, no rate limiting is enforced at the application level. For production deployments, consider:

- Traefik rate limiting middleware
- API Gateway rate limiting (AWS API Gateway)
- Custom middleware implementation

---

## Timeouts

| Operation | Timeout |
|-----------|---------|
| Request read | 10s |
| Request write | 10s |
| Request processing | 30s |
| Kafka produce | 10s |

---

## SDK Examples

### Python

```python
import requests

API_URL = "http://localhost:8080"
API_KEY = "your-api-key"

headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}

# Send match event
event = {
    "eventId": "550e8400-e29b-41d4-a716-446655440000",
    "matchId": "match-001",
    "eventType": "goal",
    "timestamp": "2024-01-15T14:30:00Z",
    "teamId": 1
}
response = requests.post(f"{API_URL}/api/events", json=event, headers=headers)
print(response.json())

# Send engagement events
engagements = {
    "events": [{
        "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "match_id": "match-001",
        "user_id": "user-123",
        "session_id": "sess-456",
        "engagement_type": "reaction",
        "game_minute": 45,
        "device_type": "mobile",
        "timestamp": "2024-01-15T14:30:01Z"
    }]
}
response = requests.post(f"{API_URL}/api/engagements", json=engagements, headers=headers)
print(response.json())
```

### JavaScript

```javascript
const API_URL = "http://localhost:8080";
const API_KEY = "your-api-key";

// Send match event
const event = {
  eventId: crypto.randomUUID(),
  matchId: "match-001",
  eventType: "goal",
  timestamp: new Date().toISOString(),
  teamId: 1
};

const response = await fetch(`${API_URL}/api/events`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
  },
  body: JSON.stringify(event)
});

console.log(await response.json());
```

### Go

```go
package main

import (
    "bytes"
    "encoding/json"
    "net/http"
)

func main() {
    event := map[string]interface{}{
        "eventId":   "550e8400-e29b-41d4-a716-446655440000",
        "matchId":   "match-001",
        "eventType": "goal",
        "timestamp": "2024-01-15T14:30:00Z",
        "teamId":    1,
    }

    body, _ := json.Marshal(event)
    req, _ := http.NewRequest("POST", "http://localhost:8080/api/events", bytes.NewBuffer(body))
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-API-Key", "your-api-key")

    client := &http.Client{}
    resp, _ := client.Do(req)
    defer resp.Body.Close()
}
```
