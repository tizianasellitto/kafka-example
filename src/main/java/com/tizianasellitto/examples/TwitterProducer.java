package com.tizianasellitto.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/*
 * TwitterProducer class is used to create a KafkaProducer instance that listens to tweets with specific keywords
 * and send their hashtag on kafka topics ready to be consumed.
 */
public class TwitterProducer {
	static LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

	public static void main(String[] args) throws IOException, InterruptedException {

		// set up the producer
		KafkaProducer<String, String> producer;
		try (final InputStream props = TwitterProducer.class.getClassLoader().getResourceAsStream("producer.props")) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}

		// Process arguments
		String CONSUMER_KEY = args[1].toString();
		String CONSUMER_SECRET = args[2].toString();
		String ACCESS_TOKEN = args[3].toString();
		String ACCESS_TOKEN_SECRET = args[4].toString();
		
		String[] arguments = args.clone();
	    String[] keywords = Arrays.copyOfRange(arguments, 5, arguments.length);


		TwitterStream twitterStream = getTwitterStreamInstance(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onException(Exception ex) {
				// TODO Auto-generated method stub
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// TODO Auto-generated method stub
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// TODO Auto-generated method stub
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// TODO Auto-generated method stub
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				// TODO Auto-generated method stub
			}

		};
		twitterStream.addListener(listener);

		// Filter tweet with specific keywords from args
		FilterQuery query = new FilterQuery().track(keywords);
		twitterStream.filter(query);

		Thread.sleep(5000);
		int i = 0;
		int j = 0;

		while (i < 100) {
			Status ret = queue.poll();

			if (ret == null) {
				Thread.sleep(1000);
				i++;
			} else {
				for (HashtagEntity hashtage : ret.getHashtagEntities()) {
					System.out.println("Hashtag: " + hashtage.getText() );
					//Get the hashtag of the tweet with specific keywords and send it on a topic. 
					producer.send(new ProducerRecord<String, String>("message-topic", Integer.toString(j++),
							hashtage.getText()));
				}
			}
		}
		producer.close();
		Thread.sleep(5000);
		twitterStream.shutdown();
	}

	/**
	 * This method is used to get a TwitterStream Instance
	 * 
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessToken
	 * @param accessTokenSecret
	 * @return 
	 */
	public static TwitterStream getTwitterStreamInstance(String consumerKey, String consumerSecret, String accessToken,
			String accessTokenSecret) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

		// Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		// Twitter twitter = TwitterFactory.getSingleton();

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		return twitterStream;
	}
}
