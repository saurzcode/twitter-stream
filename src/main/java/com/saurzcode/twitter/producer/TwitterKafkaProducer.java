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
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Producer to read from twitter for a specifc term and put on Kafka Topic.
 * Needs Twitter app configuration and access keys, token, and consumer key and secret to run.
 */
public class TwitterKafkaProducer {


    private static void run() {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList(
                TwitterKafkaConfig.TERM));

        Authentication auth = new OAuth1(
                TwitterKafkaConfig.CONSUMER_KEY,
                TwitterKafkaConfig.CONSUMER_SECRET,
                TwitterKafkaConfig.TOKEN,
                TwitterKafkaConfig.SECRET);

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

        if (!TwitterKafkaConfig.USER_NAME.isEmpty()) {
            String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
            String jaasCfg = String.format(jaasTemplate,
                    TwitterKafkaConfig.USER_NAME, TwitterKafkaConfig.PASSWORD);
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.mechanism", "SCRAM-SHA-256");
            properties.put("sasl.jaas.config", jaasCfg);
        }

        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        try {
            TwitterKafkaConfig.SetFromArgs(args);
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        // Print used config values
        TwitterKafkaConfig conf = new TwitterKafkaConfig();
        Arrays.stream(conf.getClass().getFields()).filter(
                field -> field.getGenericType() == String.class
                        && Modifier.isPublic(field.getModifiers())
                        && Modifier.isStatic(field.getModifiers()))
                .forEach(
                        field -> {
                            try {
                                System.out.println(String.format("%s: %s", field.getName(), field.get(conf)));
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        });

        TwitterKafkaProducer.run();
    }
}
