package com.mapr.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are
 * analyzed to estimate latency (assuming clock synchronization between producer
 * and consumer).
 * <p/>
 * Whenever a message is received on "slow-messages", the stats are dumped.
 */
public class Consumer {
	public static void main(String[] args) throws IOException {
		// set up house-keeping
		final ObjectMapper mapper = new ObjectMapper();
		final Histogram stats = new Histogram(1, 10000000, 2);
		final Histogram global = new Histogram(1, 10000000, 2);

		// and the consumer
		KafkaConsumer<String, String> consumer = null;
		try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
			final Properties properties = new Properties();
			properties.load(props);
			if (properties.getProperty("group.id") == null) {
				properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}
			consumer = new KafkaConsumer<>(properties);
		} catch (final Exception e) {
			e.printStackTrace();
		}
		consumer.subscribe(Arrays.asList("fast-messages", "summary-markers"));
		int timeouts = 0;
		// noinspection InfiniteLoopStatement
		while (true) {
			// read records with a short timeout. If we time out, we don't
			// really care.
			final ConsumerRecords<String, String> records = consumer.poll(200);
			if (records.count() == 0) {
				timeouts++;
			} else {
				System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
				timeouts = 0;
			}
			for (final ConsumerRecord<String, String> record : records) {
				switch (record.topic()) {
				case "fast-messages":
					// the send time is encoded inside the message
					final JsonNode msg = mapper.readTree(record.value());
					switch (msg.get("type").asText()) {
					case "test":
						final long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
						stats.recordValue(latency);
						global.recordValue(latency);
						break;
					case "marker":
						// whenever we get a marker message, we should dump out
						// the stats
						// note that the number of fast messages won't
						// necessarily be quite constant
						System.out.printf(
								"%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
								stats.getTotalCount(), stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
								stats.getMean(), stats.getValueAtPercentile(99));
						System.out.printf(
								"%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
								global.getTotalCount(), global.getValueAtPercentile(0),
								global.getValueAtPercentile(100), global.getMean(), global.getValueAtPercentile(99));

						stats.reset();
						break;
					default:
						throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
					}
					break;
				case "summary-markers":
					break;
				default:
					throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
				}
			}
		}
	}
}
