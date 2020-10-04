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
