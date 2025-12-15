import { KafkaProducer } from "./kafkaProducer.js";

const topic = "MCI_NOTIF";

export async function sendKafkaMessage({ key, value }) {
  const producer = new KafkaProducer();
  await producer.connect();

  const message = {
    key: key ?? `key-${Date.now()}`,
    value: value ?? `hello from producer at ${new Date().toISOString()}`,
  };

  console.log("Sending message:", message);
  await producer.sendMessage(topic, message);
  await producer.disconnect();
  console.log("Producer finished");
}

// Allow running this file directly: `node producer.js`
if (import.meta.url === `file://${process.argv[1]}`) {
  sendKafkaMessage({}).catch((err) => {
    console.error("Producer error:", err);
    process.exit(1);
  });
}
