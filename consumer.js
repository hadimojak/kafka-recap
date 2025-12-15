import { KafkaConsumer } from "./kafkaConsumer.js";

const topic = "MCI_NOTIF";

export async function startConsumer(onMessage = console.log) {
  const consumer = new KafkaConsumer();
  await consumer.connect();
  await consumer.subscribe(topic, { fromBeginning: true });

  console.log("Consumer started on topic:", topic);

  await consumer.run((msg) => {
    onMessage(msg);
  });
}

// Allow running this file directly: `node consumer.js`
if (import.meta.url === `file://${process.argv[1]}`) {
  startConsumer((msg) => {
    console.log("Received:", msg);
  }).catch((err) => {
    console.error("Consumer error:", err);
    process.exit(1);
  });
}
