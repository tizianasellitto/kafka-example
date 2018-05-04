package com.tizianasellitto.examples;

import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us have a
 * single executable as a build target.
 */
public class Run {
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length < 1) {
			throw new IllegalArgumentException("Must have either 'producer', 'twitterProducer' or 'consumer' as argument");
		}

		if (args[0].equals("twitterProducer") && args.length < 6 ) {
			throw new IllegalArgumentException("Must have following argument: twitterProducer twitter-consumer-key "
					+ "twitter-consumer-secret twitter-access-token twitter-access-token-secret keywords");
		}
		switch (args[0]) {
		case "producer":
			Producer.main(args);
			break;
		case "twitterProducer":
			TwitterProducer.main(args);
			break;
		case "consumer":
			Consumer.main(args);
			break;
		default:
			throw new IllegalArgumentException("Don't know how to do " + args[0]);
		}
	}
}
