package com.mapr.examples.test;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

	private static Scanner in;

	public static void main(String[] argv) throws Exception {

		final String topicName = "fast-messages";
		in = new Scanner(System.in);
		System.out.println("Enter message(type exit to quit)");

		// Configure the Producer
		final Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.2:9092");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		final Producer<String, String> producer = new KafkaProducer<String, String>(configProperties);
		String line = in.nextLine();
		while (!line.equals("exit")) {
			System.out.println(line);
			final ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
			producer.send(rec);
			System.out.println("sent");
			line = in.nextLine();
		}
		in.close();
		producer.close();
	}

}
