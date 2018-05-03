package com.tizianasellitto.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every
 * so often, it will send a message to "slow-messages". This shows how messages
 * can be sent to multiple topics. On the receiving end, we will see both kinds
 * of messages but will also see how the two topics aren't really synchronized.
 */
public class Producer {
	public static void main(String[] args) throws IOException {
		// set up the producer
		KafkaProducer<String, String> producer;
		try (final InputStream props = Producer.class.getClassLoader().getResourceAsStream("producer.props")) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}
		try {
			for (int i = 0; i < 10000; i++) {
				// send lots of messages
			    producer.send(new ProducerRecord<String, String>("message-topic", Integer.toString(i), "message " + Integer.toString(i)));
				producer.flush();
				System.out.println("Sent msg number " + i);
			}
		}finally {
			producer.close();
		}
	}
}
