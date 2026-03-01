# Rider Location API

A simple Node.js API where a rider can send their latitude and longitude.

## Setup

```bash
npm install
```

## Run

```bash
npm start
```

For development with auto-reload:

```bash
npm run dev
```

Server runs at `http://localhost:3000` (override with `PORT` env var).

## Endpoints

### POST `/rider/location`

Submit rider's current position.

**Request body (JSON):**

- `lat` / `latitude` — latitude (-90 to 90)
- `long` / `longitude` — longitude (-180 to 180)

**Example:**

```bash
curl -X POST http://localhost:3000/rider/location \
  -H "Content-Type: application/json" \
  -d '{"lat": 37.7749, "long": -122.4194}'
```

**Success (201):**

```json
{
  "success": true,
  "message": "Rider location received",
  "location": {
    "lat": 37.7749,
    "long": -122.4194,
    "receivedAt": "2025-03-01T12:00:00.000Z"
  }
}
```

### GET `/health`

Health check.

```bash
curl http://localhost:3000/health
```
