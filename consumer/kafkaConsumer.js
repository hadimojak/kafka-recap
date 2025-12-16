import { Kafka } from "kafkajs";

export class KafkaConsumer {
  constructor(kafka, { groupId = "test-group", partitionAssigner = null } = {}) {
    this.kafka = kafka;
    
    const options = { groupId };
    if (partitionAssigner) {
      options.partitionAssigners = [partitionAssigner];
    }
    
    
    this.consumer = this.kafka.consumer(options);
  }

  async connect() {
    await this.consumer.connect();
  }

  async subscribe(topic, { fromBeginning = false } = {}) {
    await this.consumer.subscribe({ topic, fromBeginning });
  }

  async waitForGroupJoin() {
    return new Promise((resolve) => {
      const handler = (event) => {
        // Remove listener after first group join for this consumer
        removeListener();
        resolve(event);
      };

      const removeListener = this.consumer.on(
        this.consumer.events.GROUP_JOIN,
        handler
      );
    });
  }

  async run(onMessage) {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const payload = {
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: message.value?.toString(),
        };

        if (onMessage) {
          await onMessage(payload);
        }
      },
    });
  }
}