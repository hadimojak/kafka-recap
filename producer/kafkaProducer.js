import { Partitioners } from "kafkajs";

export class KafkaProducer {
  constructor(kafka) {
    this.kafka = kafka;
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
  }

  async connect() {
    await this.producer.connect();
  }

  async disconnect() {
    await this.producer.disconnect();
  }

  async sendMessage(topic, message, partition = null) {
    // KafkaJS returns an array of produce responses with offsets
    const messages =
      partition !== null ? [{ ...message, partition }] : [message];

    const result = await this.producer.send({
      topic,
      messages,
    });

    return result;
  }

  async sendBulkMessage(topic, messages, partition = null) {
    const kafkaMessages = messages.map((msg) => ({
      key: msg.key,
      value: JSON.stringify(msg.value),
      partition,
    }));

    const result = await this.producer.send({
      topic,
      kafkaMessages,
    });

    return result;
  }
}

export default KafkaProducer;
