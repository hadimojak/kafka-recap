import express from "express";
import { Utils } from "./utils/index.js";
import { KafkaConnect } from "./connection/connect.js";
import { KafkaProducer } from "./producer/kafkaProducer.js";

const kafkaConnection = new KafkaConnect({
  clientId: "hadi-app",
  brokers: ["localhost:9094"],
});

const utils = new Utils();

const app = express();
const port = 3000;

const producer0 = new KafkaProducer(kafkaConnection.kafka);
const producer1 = new KafkaProducer(kafkaConnection.kafka);

app.use(express.json());

// Initialize Kafka admin & producer/consumer connections
(async () => {
  try {
    await kafkaConnection.connect();
    await kafkaConnection.createTopic("MCI_NOTIF", 2);
    await producer0.connect();
    await producer1.connect();

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
      return res.status(400).json({ error: "Array is required" });
    }

    const messages = arr.map((item) => {
      const partition = utils.selectPartition();
      const producer = partition === 1 ? producer1 : producer0;

      return {
        item,
        partition,
        producer,
      };
    });

    const partitionGroups = {};
    messages.forEach((item, partition, producer) => {
      if (!partitionGroups[partition]) {
        partitionGroups[partition] = { producer, messages: [] };
      }
      partitionGroups[partition].messages.push({
        key: item.key,
        value: item.value,
      });
    });

    //send batch per partition
console.log();
    

    res.status(200).json({
      ok: true,
      // data: {
      //   topic,
      //   key,
      //   value,
      //   partition: first?.partition,
      //   baseOffset: first?.baseOffset,
      //   logAppendTime: first?.logAppendTime,
      //   logStartOffset: first?.logStartOffset,
      // },
    });
  } catch (err) {
    console.error("Error producing message:", err);
    res.status(500).json({ error: "Failed to produce message" });
  }
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, shutting down gracefully...");
  try {
    await producer0.disconnect();
    await producer1.disconnect();
  } finally {
    process.exit(0);
  }
});

process.on("SIGINT", async () => {
  console.log("SIGINT received, shutting down gracefully...");
  try {
    await producer0.disconnect();
    await producer1.disconnect();
  } finally {
    process.exit(0);
  }
});
