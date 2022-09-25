package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {


    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";
        //kafkaProducer configuration setting
        //null, "hello world"

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serialized.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${IP}:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        //ProducerRecord object creation
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "id-001", "hello world");

        //KafkaProducer send message
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ##### record metadata received ###### \n" +
                    "partition:" + recordMetadata.partition() + "\n" +
                    "offset:" + recordMetadata.offset() + "\n" +
                    "timestamp:" + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.close();
        }
    }
}
