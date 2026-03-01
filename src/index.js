const express = require("express");
const { connectProducer, sendRiderLocation, runConsumer, disconnect } = require("./kafka");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

/**
 * POST /rider/location
 * Body: { "lat": number, "long": number } or { "latitude": number, "longitude": number }
 * Accepts rider's current latitude and longitude.
 */
app.post("/rider/location", async (req, res) => {
  const lat = req.body.lat ?? req.body.latitude;
  const long = req.body.long ?? req.body.longitude;

  if (lat == null || long == null) {
    return res.status(400).json({
      error: "Missing coordinates",
      message:
        'Provide "lat" and "long" (or "latitude" and "longitude") in the request body.',
    });
  }

  const latNum = Number(lat);
  const longNum = Number(long);

  if (Number.isNaN(latNum) || Number.isNaN(longNum)) {
    return res.status(400).json({
      error: "Invalid coordinates",
      message: "lat and long must be numbers.",
    });
  }

  if (latNum < -90 || latNum > 90) {
    return res.status(400).json({
      error: "Invalid latitude",
      message: "Latitude must be between -90 and 90.",
    });
  }

  if (longNum < -180 || longNum > 180) {
    return res.status(400).json({
      error: "Invalid longitude",
      message: "Longitude must be between -180 and 180.",
    });
  }

  // Produce to Kafka; consumer buffers and bulk-inserts (logs) every 1 min
  const location = {
    lat: latNum,
    long: longNum,
    receivedAt: new Date().toISOString(),
    ...(req.body.riderId != null && { riderId: req.body.riderId }),
  };

  try {
    await sendRiderLocation(location);
  } catch (err) {
    console.error("Kafka produce error:", err);
    return res.status(503).json({
      error: "Service unavailable",
      message: "Could not enqueue location. Try again later.",
    });
  }

  res.status(201).json({
    success: true,
    message: "Rider location received",
    location,
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`Rider location API listening on http://localhost:${PORT}`);
  (async () => {
    try {
      await connectProducer();
      await runConsumer();
    } catch (err) {
      console.error("Kafka connect error:", err.message || err);
      console.error(
        "Ensure Kafka is running (e.g. 127.0.0.1:9092). Set KAFKA_BROKERS. API is up but locations will return 503 until Kafka is available.",
      );
    }
  })();
});

function shutdown() {
  disconnect()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error("Shutdown error:", err);
      process.exit(1);
    });
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
