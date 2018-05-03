# kafka-examples
Snippets and small examples demonstrating Kafka features.


# Quickstart
Launch Zookeeper and  Kafka before running the example.

```
# Start Zookeeper in its own terminal.
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```

```
# Start Kafka, also in its own terminal.
$ bin/kafka-server-start.sh config/server.properties 
```

Then create a "message-topic" topic.

```
# Create "message-topic" topic
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic message-topic
```
At this point Zookeeper, Kafka are aup and running.

Now we can run our application (Producer and Consumer).

First go to the example folder and launch the app using "producer" or "consumer" as args. The application use maven to create an executable jar that is in the target folder.
```
$ mvn clean install
$ target/kafka-example producer
# Run the producer
```
```
$ target/kafka-example consumer
# Run the consumer
```
