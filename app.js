import express from "express";
import pMap from "p-map";
import { Utils } from "./utils/index.js";
import { KafkaConnect } from "./connection/connect.js";
import { KafkaProducer } from "./producer/kafkaProducer.js";
import { KafkaConsumer } from "./consumer/kafkaConsumer.js";

const kafkaConnection = new KafkaConnect({
  clientId: "hadi-app",
  brokers: ["localhost:9094"],
});

const utils = new Utils();

const app = express();
const port = 3000;

const producer0 = new KafkaProducer(kafkaConnection.kafka);
const producer1 = new KafkaProducer(kafkaConnection.kafka);

const consumer0 = new KafkaConsumer(kafkaConnection.kafka, {
  groupId: "hadi-consumer-0",
});
const consumer1 = new KafkaConsumer(kafkaConnection.kafka, {
  groupId: "hadi-consumer-1",
});

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

// Initialize Kafka admin & producer/consumer connections
(async () => {
  try {
    await kafkaConnection.connect();
    await kafkaConnection.createTopic("MCI_NOTIF", 2);
    await producer0.connect();
    await producer1.connect();
    await consumer0.connect();
    await consumer1.connect();

    await consumer0.assign("MCI_NOTIF", [0]);
    await consumer1.assign("MCI_NOTIF", [1]);

    consumer0.run((payload) => {
      try {
        const valueObj = JSON.parse(payload.value);
        console.log(`consumer0 parsed value : ${valueObj}`);
      } catch (error) {
        console.log("Error parsing value object:", error);
      }
    });

    consumer1.run((payload) => {
      try {
        const valueObj = JSON.parse(payload.value);
        console.log(`consumer1 parsed value : ${valueObj}`);
      } catch (error) {
        console.log("Error parsing value object:", error);
      }
    });

    app
      .listen(port, () => {
        console.log(`Express server listening on http://localhost:${port}`);
      })
      .on("error", (err) => {
        console.error("Express server error:", err);
      });
  } catch (err) {
    console.error("Failed to initialize Kafka:", err);
  }
})();

// HTTP endpoint to send a message to Kafka
app.post("/produce", async (req, res) => {
  try {
    const { topic = "MCI_NOTIF", key, value } = req.body;

    const targetPartition = utils.selectPartition();
    const valueString = JSON.stringify(value);
    const producer = targetPartition === 1 ? producer1 : producer0;

    const result = await producer.sendMessage(
      topic,
      {
        key: key,
        value: valueString,
      },
      targetPartition
    );

    const [first] = result;

    res.status(200).json({
      ok: true,
      data: {
        topic,
        key,
        value,
        partition: first?.partition,
        baseOffset: first?.baseOffset,
        logAppendTime: first?.logAppendTime,
        logStartOffset: first?.logStartOffset,
      },
    });
  } catch (err) {
    console.error("Error producing message:", err);
    res.status(500).json({ error: "Failed to produce message" });
  }
});

app.post("/produce/bulk", async (req, res) => {
  try {
    const { topic = "MCI_NOTIF", arr } = req.body;

    if (!Array.isArray(arr) || arr.length === 0) {
      return res.status(400).json({ error: "arr must be a non-empty array" });
    }

    const CHUNK_SIZE = 100; // Process 100 messages per chunk
    const CONCURRENCY = 5; // Process 5 chunks concurrently

    // Split array into chunks
    const chunks = [];
    for (let i = 0; i < arr.length; i += CHUNK_SIZE) {
      chunks.push(arr.slice(i, i + CHUNK_SIZE));
    }

    // Process chunks with concurrency limit using p-map
    const results = await pMap(
      chunks,
      async (chunk) => {
        // Group chunk messages by partition
        const partitionGroups = {};

        chunk.forEach((item) => {
          const partition = utils.selectPartition();
          const producer = partition === 1 ? producer1 : producer0;

          if (!partitionGroups[partition]) {
            partitionGroups[partition] = {
              producer,
              messages: [],
            };
          }

          partitionGroups[partition].messages.push({
            key: item.key,
            value: item.value,
          });
        });

        // Send batches for each partition in this chunk
        const batchResults = await Promise.all(
          Object.entries(partitionGroups).map(
            ([partition, { producer, messages }]) =>
              producer.sendBatch(topic, messages, parseInt(partition))
          )
        );

        return batchResults.reduce((sum, result) => sum + result.length, 0);
      },
      { concurrency: CONCURRENCY }
    );

    const totalSent = results.reduce((sum, count) => sum + count, 0);

    res.status(200).json({
      ok: "true",
      sent: "totalSent",
      chunks: "chunks.length",
      concurrency: "CONCURRENCY",
    });
  } catch (err) {
    console.error("Error producing bulk messages:", err);
    res.status(500).json({ error: "Failed to produce bulk messages" });
  }
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, shutting down gracefully...");
  try {
    await producer0.disconnect();
    await producer1.disconnect();
    await consumer0.disconnect();
    await consumer1.disconnect();
    await kafkaConnection.disconnect();
  } finally {
    process.exit(0);
  }
});

process.on("SIGINT", async () => {
  console.log("SIGINT received, shutting down gracefully...");
  try {
    await producer0.disconnect();
    await producer1.disconnect();
    await consumer0.disconnect();
    await consumer1.disconnect();
    await kafkaConnection.disconnect();
  } finally {
    process.exit(0);
  }
});
