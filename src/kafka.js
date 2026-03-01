const { Kafka } = require("kafkajs");

if (!process.env.KAFKAJS_NO_PARTITIONER_WARNING) {
  process.env.KAFKAJS_NO_PARTITIONER_WARNING = "1";
}

const rawBrokers = process.env.KAFKA_BROKERS || "127.0.0.1:9092";
const BROKERS = rawBrokers
  .split(",")
  .map((b) => b.trim())
  .map((b) => (b.includes(":") ? b : `${b}:9092`));
const TOPIC = process.env.KAFKA_TOPIC_RIDER_LOCATIONS || "rider-locations";
const BULK_FLUSH_INTERVAL_MS =
  Number(process.env.BULK_FLUSH_INTERVAL_MS) || 60_000;
const BULK_FLUSH_MAX_SIZE = Number(process.env.BULK_FLUSH_MAX_SIZE) || 5000;

const kafka = new Kafka({
  clientId: "rider-location-api",
  brokers: BROKERS,
});

const producer = kafka.producer();
let consumer = null;

async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = await admin.listTopics();
    if (!existing.includes(TOPIC)) {
      try {
        await admin.createTopics({
          topics: [{ topic: TOPIC, numPartitions: 1 }],
        });
        console.log(`Kafka topic "${TOPIC}" created.`);
      } catch (createErr) {
        console.warn(
          `Could not create topic "${TOPIC}":`,
          createErr.message || createErr,
        );
      }
    }
  } finally {
    await admin.disconnect();
  }
}

function getConsumer() {
  if (!consumer) {
    consumer = kafka.consumer({
      groupId: "rider-location-bulk-insert",
      sessionTimeout: 30_000,
      rebalanceTimeout: 60_000,
    });
  }
  return consumer;
}

async function connectProducer() {
  await ensureTopic();
  await producer.connect();
}

async function sendRiderLocation(location) {
  await producer.send({
    topic: TOPIC,
    messages: [
      {
        value: JSON.stringify(location),
        key: location.riderId ? String(location.riderId) : null,
      },
    ],
  });
}

/** Buffer messages; flush every BULK_FLUSH_INTERVAL_MS or when buffer reaches BULK_FLUSH_MAX_SIZE */
const buffer = [];
let flushTimer = null;
let flushing = false;

async function flushBuffer() {
  if (buffer.length === 0 || flushing) return;

  flushing = true;
  const batch = buffer.splice(0, buffer.length);

  try {
    const locations = batch.map((e) => {
      try {
        return JSON.parse(e.value.toString());
      } catch {
        return { raw: e.value.toString() };
      }
    });

    // Replace with your DB bulk insert; on throw we put batch back and don't commit
    console.log(
      `[BULK INSERT] ${locations.length} rider location(s) at ${new Date().toISOString()}:`,
      JSON.stringify(locations, null, 2),
    );

    const c = getConsumer();
    const maxByPartition = new Map();
    for (const { topic, partition, offset } of batch) {
      const key = `${topic}|${partition}`;
      const num = Number(offset);
      const cur = maxByPartition.get(key);
      if (!cur || num > cur.maxOffset) {
        maxByPartition.set(key, { topic, partition, maxOffset: num });
      }
    }
    const offsets = Array.from(maxByPartition.values()).map(
      ({ topic, partition, maxOffset }) => ({
        topic,
        partition,
        offset: String(maxOffset + 1),
      }),
    );
    if (offsets.length > 0) {
      await c.commitOffsets(offsets);
    }
  } catch (err) {
    console.error("[BULK INSERT] error, will retry batch:", err.message || err);
    buffer.unshift(...batch);
  } finally {
    flushing = false;
  }
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setInterval(async () => {
    await flushBuffer();
  }, BULK_FLUSH_INTERVAL_MS);
  flushTimer.unref?.();
}

async function runConsumer() {
  const c = getConsumer();
  await c.connect();
  await c.subscribe({ topic: TOPIC, fromBeginning: true });

  await c.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      buffer.push({
        value: message.value,
        topic,
        partition,
        offset: message.offset,
      });
      if (buffer.length >= BULK_FLUSH_MAX_SIZE) {
        await flushBuffer();
      }
    },
  });

  scheduleFlush();
  console.log(
    `Kafka consumer subscribed to "${TOPIC}", flush every ${BULK_FLUSH_INTERVAL_MS / 1000}s or when buffer >= ${BULK_FLUSH_MAX_SIZE}.`,
  );
}

async function disconnect() {
  await flushBuffer();
  await producer.disconnect();
  if (consumer) await consumer.disconnect();
}

module.exports = {
  connectProducer,
  sendRiderLocation,
  runConsumer,
  disconnect,
  TOPIC,
};
