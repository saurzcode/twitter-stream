[![Build Status](https://travis-ci.org/saurzcode/twitter-stream.svg?branch=master)](https://travis-ci.org/saurzcode/twitter-stream)
[![GitHub stars](https://img.shields.io/github/stars/saurzcode/twitter-stream.svg?style=social&label=Star&maxAge=2592000)](https://GitHub.com/saurzcode/twitter-stream/stargazers/)
[![GitHub forks](https://img.shields.io/github/forks/saurzcode/twitter-stream.svg?style=social&label=Fork&maxAge=2592000)](https://GitHub.com/saurzcode/twitter-stream/network/)
[![GitHub watchers](https://img.shields.io/github/watchers/saurzcode/twitter-stream.svg?style=social&label=Watch&maxAge=2592000)](https://GitHub.com/saurzcode/twitter-stream/watchers/)
[![GitHub contributors](https://img.shields.io/github/contributors/saurzcode/twitter-stream.svg)](https://GitHub.com/saurzcode/twitter-stream/graphs/contributors/)

# twitter-stream
Twitter-Kafka Data Pipeline

# Requirements :

Apache Kafka 2.6.0
Twitter Developer account ( for API Key, Secret etc.)
Apache Zookeeper ( required for Kafka)
Oracle JDK 1.8 (64 bit )


# How to Run
Provide JVM Argument for TwitterKafkaProducer.java in following order

java TwitterKafkaProducer.java <consumer_key> <consumer_secret> <account_token> <account_secret> <hashtag/term>
                               			                               			 
You can configure name of the topic in [TwittterKafkaConfig.java](src/main/java/com/saurzcode/twitter/config/TwitterKafkaConfig.java)
# Build Environment :
Eclipse/Intellij
Apache Maven 

Detailed steps available here - 
http://saurzcode.in/2015/02/kafka-producer-using-twitter-stream/
