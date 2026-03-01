# Rider Location API

Riders POST lat/long to the API. Locations are sent to Kafka; a consumer buffers them and bulk-inserts (e.g. to a DB) on an interval or when the buffer is full—so you avoid per-request DB writes.

**Run:** `npm install` then `npm run dev`. API: `http://localhost:3000`.  
**Kafka:** Start with `docker compose up -d`. Set `KAFKA_BROKERS` if not using default `127.0.0.1:9092`.

- **POST `/rider/location`** — Body: `{"lat": number, "long": number}`. Returns 201 and enqueues to Kafka.
- **GET `/health`** — Health check.

Env: `PORT`, `KAFKA_BROKERS`, `KAFKA_TOPIC_RIDER_LOCATIONS`, `BULK_FLUSH_INTERVAL_MS`, `BULK_FLUSH_MAX_SIZE`.
