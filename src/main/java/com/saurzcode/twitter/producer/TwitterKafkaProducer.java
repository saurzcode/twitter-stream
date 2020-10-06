package com.saurzcode.twitter.producer;

import com.google.common.collect.Lists;
import com.saurzcode.twitter.config.TwitterKafkaConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Producer to read from twitter for a specifc term and put on Kafka Topic.
 * Needs Twitter app configuration and access keys, token, and consumer key and secret to run.
 */
public class TwitterKafkaProducer {


    private static void run(String consumerKey, String consumerSecret,
                            String token, String secret, String term) {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList(
                term));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);

        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();


        client.connect();
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                ProducerRecord<Long, String> message = new ProducerRecord<>(TwitterKafkaConfig.TOPIC, queue.take());
                producer.send(message);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    private static Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TwitterKafkaConfig.SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {

        if (args.length != 5)
            throw new IllegalArgumentException("Please Pass 5 arguments, in order - consumerKey, consumerSecret, token, secret, and term");
        //These should be passed in VM arguments for the application.
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String token = args[2];
        String secret = args[3];
        String term = args[4]; // term on twitter on which you want to filter the results on.

        TwitterKafkaProducer.run(consumerKey, consumerSecret, token, secret, term);

    }
}
