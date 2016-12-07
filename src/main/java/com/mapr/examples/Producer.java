package com.mapr.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every
 * so often, it will send a message to "slow-messages". This shows how messages
 * can be sent to multiple topics. On the receiving end, we will see both kinds
 * of messages but will also see how the two topics aren't really synchronized.
 */
public class Producer {
	public static void main(String[] args) throws IOException {
		// set up the producer
		KafkaProducer<String, String> producer = null;
		try (InputStream props = Resources.getResource("producer.properties").openStream()) {
			final Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
			System.out.println(producer);
		} catch (final Exception e) {
			e.printStackTrace();
		}

		try {
			for (long i = 0; i < 1000; i++) {
				System.out.println(i);
				// send lots of messages
				producer.send(new ProducerRecord<String, String>("fast-messages",
						String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
				System.out.println("message sent");
				// every so often send to a different topic
				if (i % 1000 == 0) {
					producer.send(new ProducerRecord<String, String>("fast-messages",
							String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
					producer.send(new ProducerRecord<String, String>("summary-markers",
							String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
					producer.flush();
					System.out.println("Sent msg number " + i);
				}
			}
		} catch (final Throwable throwable) {
			throwable.printStackTrace();
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			System.out.println("producer closed");
			producer.close();
		}

	}
}
