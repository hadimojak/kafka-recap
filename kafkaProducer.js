import { Kafka } from "./connect.js";

export class KafkaProducer {
  constructor({ clientId = "hadi_producer", brokers = ["localhost:9094"] } = {}) {
    this.kafka = new Kafka({ clientId, brokers });
    this.producer = this.kafka.producer();
  }

  async connect() {
    await this.producer.connect();
  }

  async disconnect() {
    await this.producer.disconnect();
  }

  async sendMessage(topic, { key, value }) {
    await this.producer.send({
      topic,
      messages: [{ key, value }],
    });
  }
}

