import { Kafka } from "kafkajs";
import express, { Express } from "express";
import morgan from "morgan";

const kafka = new Kafka({
  clientId: "ts-app",
  brokers: ["kafka:9092"],
});

class Order {
  private products: string[];
  private total: number;
  constructor(products: string[], total: number) {
    this.products = products;
    this.total = total;
  }
}

const bootstrap = async (app: Express) => {
  const producer = kafka.producer();
  await producer.connect();

  app.use(express.json());
  app.use(morgan("dev"));
  app.post("/order", async (req, res) => {
    const order = req.body;
    console.log("order created: ", order);
    await producer.send({
      topic: "order-topic",
      messages: [{ value: JSON.stringify(order) }],
    });
    res.status(200).send("Order sent to Kafka");
  });
  app.listen(3000, () => {
    console.log("Order service running on port 3000");
  });
};

const app = express();
bootstrap(app);
