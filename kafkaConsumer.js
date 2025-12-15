import { Kafka } from "./connect.js";

export class KafkaConsumer {
  constructor({
    clientId = "hadi-consumer",
    brokers = ["localhost:9094"],
    groupId = "test-group",
  } = {}) {
    this.kafka = new Kafka({ clientId, brokers });
    this.consumer = this.kafka.consumer({ groupId });
  }

  async connect() {
    await this.consumer.connect();
  }

  async subscribe(topic, { fromBeginning = false } = {}) {
    await this.consumer.subscribe({ topic, fromBeginning });
  }

  async run(onMessage) {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await onMessage({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: message.value?.toString(),
        });
      },
    });
  }
}

