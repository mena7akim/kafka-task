import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "ts-app",
  brokers: ["kafka:9092"],
});

const consumer = kafka.consumer({ groupId: "ts-group" });

const run = async () => {
  await consumer.connect();

  await consumer.subscribe({ topic: "order-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("order received: ", message.value?.toString());
      console.log("Sending email...");
      setTimeout(() => {
        console.log("Email sent!");
      }, 2000);
    },
  });
};

run().catch(console.error);
