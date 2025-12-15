import { Kafka } from "kafkajs";

async function main() {
  const kafka = new Kafka({
    clientId: "test-client",
    brokers: ["localhost:9094"], // <– use 9094 now
  });

  const admin = kafka.admin();
  await admin.connect();

  const topics = await admin.listTopics();
  if (topics.length) { console.log("kafka successfully connected \n topics: ", topics); }

  await admin.disconnect();
}

export { Kafka, main };