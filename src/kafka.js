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
      maxWaitTimeInMs: 10000, // wait up to 60s to fill a batch
      // minBytes: 1024 * 1024, // wait until ~1MB of messages is ready
      // maxBytes: 10 * 1024 * 1024, // max 10MB per batch fetch
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

async function runConsumer() {
  const c = getConsumer();
  await c.connect();
  await c.subscribe({ topic: TOPIC, fromBeginning: true });

  await c.run({
    autoCommit: false,
    eachBatchAutoResolve: false,

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      const { topic, partition, messages } = batch;

      // Parse all messages in the batch
      const locations = messages.map((msg) => {
        try {
          return JSON.parse(msg.value.toString());
        } catch {
          return { raw: msg.value.toString() };
        }
      });

      if (locations.length === 0) return;

      // ✅ YOUR BULK INSERT GOES HERE
      console.log(
        `[BULK INSERT] ${locations.length} rider location(s) at ${new Date().toISOString()}:`,
        JSON.stringify(locations, null, 2),
      );

      // Commit offset of last message after successful insert
      if (isRunning() && !isStale()) {
        const lastMessage = messages[messages.length - 1];
        resolveOffset(lastMessage.offset);
        await heartbeat();

        await c.commitOffsets([
          {
            topic,
            partition,
            offset: String(Number(lastMessage.offset) + 1),
          },
        ]);
      }
    },
  });

  console.log(
    `Kafka consumer subscribed to "${TOPIC}", using built-in eachBatch.`,
  );
}

async function disconnect() {
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
