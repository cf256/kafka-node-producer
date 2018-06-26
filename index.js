const kafka = require("kafka-node");
const Producer = kafka.Producer;

class KafkaProducer {
  constructor(asyncResult, topic, producer) {
    if (!asyncResult || !asyncResult.ready) {
      console.log(asyncResult);
      console.log("No async result");
      throw new Error("Dont call directly");
    }

    if (!topic) {
      console.log("no topic");
      throw new Error("Pass in a topic");
    }

    this._topic = topic;
    this._producer = producer;
  }

  onReady() {
    console.log("I'm Ready!");
    this._ready = true;
  }

  onError(error) {
    this._ready = false;
    console.error(error);

    throw new Error("Producer error", error);
  }

  isReady() {
    return this._ready;
  }

  static getReady(producer) {
    return new Promise((resolve, reject) => {
      producer.on("ready", () => {
        this._isReady = true;
        console.log("Producer is ready");
        return resolve({ ready: true });
      });

      producer.on("error", error => {
        this._isReady = false;
        console.log("Producer is not ready");
        console.error(error);
        return reject({ ready: false });
      });
    });
  }

  /**
   * @param  {string} topic
   */
  static async build(topic) {
    try {
      const client = kafka.Client(
        process.env.KAFKA_CONNECTION_STRING || "localhost:2181/",
        process.env.KAFKA_CLIENT_ID || "node-kafka-producer"
      );

      const producer = new Producer(client, { requireAcks: 1 });
      const result = await this.getReady(producer);
      return new KafkaProducer(result, topic, producer);
    } catch (error) {
      console.error(topic);
      console.error(error);
      throw new Error(error);
    }
  }

  send(message) {
    try {
      return this._producer.send(
        [
          {
            topic: this._topic,
            messages: JSON.stringify(message)
          }
        ],
        (err, result) => {
          if (err) {
            throw new Error("Failed to produce message");
            return false;
          }
          console.log("Sent message to queue");
          console.log(result);
          return true;
        }
      );
    } catch (error) {
      console.error(error);
    }
  }
}

module.exports = KafkaProducer;
