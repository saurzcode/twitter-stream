package com.saurzcode.twitter.config;

import org.apache.commons.cli.*;

public class TwitterKafkaConfig {
    final static String TopicOption = "topic";
    final static String ServersOption = "servers";
    final static String UserNameOption = "username";
    final static String PasswordOption = "password";

    public static String CONSUMER_KEY = "";
    public static String CONSUMER_SECRET = "";
    public static String TOKEN = "";
    public static String SECRET = "";
    public static String TERM = "";

    public static String SERVERS = "localhost:9092";
    public static String TOPIC = "twitter-topic";

    // For Kafka SASL Auth
    public static String USER_NAME = "";
    public static String PASSWORD = "";

    public static void SetFromArgs(String[] args) throws ParseException, IllegalArgumentException {
        Options options = new Options();
        options
                .addOption(new Option(TopicOption, true, "Kafka topic"))
                .addOption(new Option(ServersOption, true, "Comma separated list of Kafka bootstrap servers"))
                .addOption(new Option(UserNameOption, true, "SASL Auth username"))
                .addOption(new Option(PasswordOption, true, "SASL Auth password"));
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption(ServersOption)) {
                TwitterKafkaConfig.SERVERS = cmd.getOptionValue(ServersOption);
            }

            if (cmd.hasOption(TopicOption)) {
                TwitterKafkaConfig.TOPIC = cmd.getOptionValue(TopicOption);
            }

            if (cmd.hasOption(UserNameOption)) {
                TwitterKafkaConfig.USER_NAME = cmd.getOptionValue(UserNameOption);
            }

            if (cmd.hasOption(PasswordOption)) {
                TwitterKafkaConfig.PASSWORD = cmd.getOptionValue(PasswordOption);
            }

            String[] remaining = cmd.getArgs();
            if (remaining.length != 5)
                throw new IllegalArgumentException("Please Pass 5 arguments, in order - consumerKey, consumerSecret, token, secret, and term");
            //These should be passed in VM arguments for the application.
            TwitterKafkaConfig.CONSUMER_KEY = remaining[0];
            TwitterKafkaConfig.CONSUMER_SECRET = remaining[1];
            TwitterKafkaConfig.TOKEN = remaining[2];
            TwitterKafkaConfig.SECRET = remaining[3];
            TwitterKafkaConfig.TERM = remaining[4]; // term on twitter on which you want to filter the results on.
        } catch (ParseException e) {
            formatter.printHelp("twitter-stream", options);

            throw e;
        }
    }
}
