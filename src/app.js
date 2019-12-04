import 'dotenv/config';

import { Kafka, logLevel, CompressionTypes } from 'kafkajs';
import Agenda from 'agenda';

class App {
  constructor() {
    this.agenda = new Agenda({ db: { address: process.env.MONGO_URL } });

    this.kafka = new Kafka({
      clientId: 'scheduling',
      brokers: [process.env.KAFKA_URL],
      logLevel: logLevel.WARN,
    });

    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: 'scheduling-group' });

    this.defineJobs();

    this.run();
  }

  getFormatedDate() {
    const time = new Date();
    const formated = `${time.getHours()}h ${time.getMinutes()}m ${time.getSeconds()}s`;

    return formated;
  }

  async defineJobs() {
    this.agenda.define('VERIFY_FILTER', async job => {
      const { aquarium } = job.attrs.data;
      const formated = this.getFormatedDate();

      const message = { type: 'NOTIFY_TO_VERIFY_FILTER', aquarium };

      await this.producer.send({
        topic: 'scheduling-notification',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(message) }],
      });

      console.log(
        `Command: VERIFY_FILTER - Aquarium: ${aquarium} - Time: ${formated}`
      );
    });

    this.agenda.define('VERIFY_FOOD', async job => {
      const { aquarium } = job.attrs.data;
      const formated = this.getFormatedDate();

      const message = { type: 'NOTIFY_TO_VERIFY_FOOD', aquarium };

      await this.producer.send({
        topic: 'scheduling-notification',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(message) }],
      });

      console.log(
        `Command: VERIFY_FOOD - Aquarium: ${aquarium} - Time: ${formated}`
      );
    });

    this.agenda.define('SWAP_WATER', async job => {
      const { aquarium } = job.attrs.data;
      const formated = this.getFormatedDate();

      const message = { type: 'NOTIFY_TO_SWAP_WATER', aquarium };

      await this.producer.send({
        topic: 'scheduling-notification',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(message) }],
      });

      console.log(
        `Command: SWAP_WATER - Aquarium: ${aquarium} - Time: ${formated}`
      );
    });

    this.agenda.define('TURN_ON_LIGHTS', async job => {
      const { aquarium } = job.attrs.data;
      const formated = this.getFormatedDate();

      const message = { type: 'REQUEST_TURN_ON_LIGHTS', aquarium };

      await this.producer.send({
        topic: 'scheduling-websocket',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(message) }],
      });

      const notify = { type: 'NOTIFY_TURN_ON_LIGHTS_REQUEST', aquarium };

      await this.producer.send({
        topic: 'scheduling-notification',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(notify) }],
      });

      console.log(
        `Command: TURN_ON_LIGHTS - Aquarium: ${aquarium} - Time: ${formated}`
      );
    });

    this.agenda.define('TURN_OFF_LIGHTS', async job => {
      const { aquarium } = job.attrs.data;
      const formated = this.getFormatedDate();

      const message = { type: 'REQUEST_TURN_OFF_LIGHTS', aquarium };

      await this.producer.send({
        topic: 'scheduling-websocket',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(message) }],
      });

      const notify = { type: 'NOTIFY_TURN_OFF_LIGHTS_REQUEST', aquarium };

      await this.producer.send({
        topic: 'scheduling-notification',
        compression: CompressionTypes.GZIP,
        messages: [{ value: JSON.stringify(notify) }],
      });

      console.log(
        `Command: TURN_OFF_LIGHTS - Aquarium: ${aquarium} - Time: ${formated}`
      );
    });
  }

  async run() {
    await this.producer.connect();
    await this.consumer.connect();

    await this.consumer.subscribe({ topic: 'aquarium-scheduling' });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        console.log('Request: ', String(message.value));

        const payload = JSON.parse(message.value);
        const { type, params, body } = payload;

        let verifyFilter;
        let verifyFood;
        let swapWater;
        let turnOnLights;
        let turnOffLights;

        switch (type) {
          case 'CREATE_JOBS':
            verifyFilter = this.agenda.create('VERIFY_FILTER', {
              aquarium: params.name,
            });

            verifyFood = this.agenda.create('VERIFY_FOOD', {
              aquarium: params.name,
            });

            swapWater = this.agenda.create('SWAP_WATER', {
              aquarium: params.name,
            });

            turnOnLights = this.agenda.create('TURN_ON_LIGHTS', {
              aquarium: params.name,
            });

            turnOffLights = this.agenda.create('TURN_OFF_LIGHTS', {
              aquarium: params.name,
            });

            verifyFilter.schedule('today at 8:10am').repeatEvery('30 days');
            verifyFood.schedule('today at 8:20am').repeatEvery('7 days');
            swapWater.schedule('today at 8:30am').repeatEvery('15 days');
            turnOnLights
              .schedule(`today at ${body.turnOnLights}am`)
              .repeatEvery('day');
            turnOffLights
              .schedule(`today at ${body.turnOffLights}am`)
              .repeatEvery('day');

            await verifyFilter.save();
            await verifyFood.save();
            await swapWater.save();
            await turnOnLights.save();
            await turnOffLights.save();

            this.agenda.define(`FEED_FISHES_${params.name}`, async job => {
              const { aquarium } = job.attrs.data;
              const formated = this.getFormatedDate();

              const newMessage = { type: 'REQUEST_FEED_FISHES', aquarium };

              await this.producer.send({
                topic: 'scheduling-websocket',
                compression: CompressionTypes.GZIP,
                messages: [{ value: JSON.stringify(newMessage) }],
              });

              const notify = { type: 'NOTIFY_FISH_FEED_REQUEST', aquarium };

              await this.producer.send({
                topic: 'scheduling-notification',
                compression: CompressionTypes.GZIP,
                messages: [{ value: JSON.stringify(notify) }],
              });

              console.log(
                `Command: FEED_FISHES - Aquarium: ${aquarium} - Time: ${formated}`
              );
            });

            await this.agenda.every(
              `${body.feedInterval} hours`,
              `FEED_FISHES_${params.name}`,
              {
                aquarium: params.name,
              }
            );

            break;
          default:
            console.log(`Message type ${type} is invalid!`);
            break;
        }
      },
    });
  }
}

export default new App().agenda;
