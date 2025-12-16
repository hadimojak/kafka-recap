import { Kafka } from "kafkajs";

class KafkaConnect {
  constructor({ clientId, brokers }) {
    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8,
        maxRetryTime: 30000,
        multiplier: 2,
      },
      connectionTimeout: 3000,
      requestTimeout: 30000,
      enforceRequestTimeout: true,
    });
    this.admin = this.kafka.admin();
    this.topics = [];
  }

  async connect() {
    await this.admin.connect();
  }

  async disconnect() {
    await this.admin.disconnect();
  }

  async createTopic(topic, numPartitions) {
    const topics = await this.admin.listTopics();

    if (topics.includes(topic)) return; // Topic already exists, no need to create

    await this.admin.createTopics({
      topics: [
        {
          topic,
          numPartitions: numPartitions,
        },
      ],
      waitForLeaders: true,
    });

  }

  async getPartitionCount(topic) {
    const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
    const topicMetadata = metadata.topics.find((t) => t.name === topic);
    if (!topicMetadata) {
      throw new Error(`Topic ${topic} not found`);
    }
    return topicMetadata.partitions.length;
  }

  async deleteTopic(topic) {
    await this.admin.deleteTopics({ topics: [topic] });
  }
}

export { KafkaConnect };
