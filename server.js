import express from "express";
import { startConsumer } from "./consumer.js";
import { sendKafkaMessage } from "./producer.js";

const app = express();
const port = 3000;

app.use(express.json());

// Start the Kafka consumer from consumer.js
async function startKafka() {
  await startConsumer((msg) => {
    console.log("Consumer received:", msg);
  });
}

// HTTP endpoint to send a message to Kafka using producer.js
app.post("/produce", async (req, res) => {
  try {
    console.log(req.body);
    
    const { key, value } = req.body;

    if (typeof value !== "string") {
      return res
        .status(400)
        .json({ error: "Body must include at least `value` as string" });
    }

    await sendKafkaMessage({ key, value });
    return res.status(200).json({ status: "ok", value });
  } catch (err) {
    console.error("Error in /produce:", err);
    return res.status(500).json({ error: "Failed to produce message" });
  }
});

app
  .listen(port, async () => {
    console.log(`Express server listening on http://localhost:${port}`);
    try {
      await startKafka();
      console.log("Kafka consumer started (from consumer.js)");
    } catch (err) {
      console.error("Failed to start Kafka consumer:", err);
    }
  })
  .on("error", (err) => {
    console.error("Express server error:", err);
  });

