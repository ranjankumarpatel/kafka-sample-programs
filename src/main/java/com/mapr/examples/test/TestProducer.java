package com.mapr.examples.test;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {

	public TestProducer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		final long events = Long.parseLong(args[0]);
		final Random rnd = new Random();

		final Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		// final ProducerConfig config = new ProducerConfig(props);

		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			final long runtime = new Date().getTime();
			final String ip = "192.168.2." + rnd.nextInt(255);
			final String msg = runtime + ",www.example.com," + ip;
			// final KeyedMessage<String, String> data = new
			// KeyedMessage<String, String>("page_visits", ip, msg);
			producer.send(new ProducerRecord<String, String>("fast-messages", ip, msg));
		}
		producer.close();

	}

}
