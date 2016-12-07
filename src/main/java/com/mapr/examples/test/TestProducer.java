package com.mapr.examples.test;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {

	public TestProducer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		final long events = 100l;
		final Random rnd = new Random();

		final Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.0.2:6667");
		props.put("bootstrap.servers", "192.168.0.2:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		final ProducerConfig config = new ProducerConfig(props);
		final Producer<String, String> producer = new Producer<String, String>(config);

		final KafkaProducer<String, String> producerk = new KafkaProducer<String, String>(props);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			final long runtime = new Date().getTime();
			final String ip = "192.168.2." + rnd.nextInt(255);
			final String msg = runtime + ",www.example.com," + ip;
			final KeyedMessage<String, String> data = new KeyedMessage<String, String>("fast-messages", ip, msg);
			System.out.println(ip + " , " + msg);
			producer.send(data);
			producerk.send(new ProducerRecord<String, String>("fast-messages", ip, msg));
		}
		producer.close();

	}

}
