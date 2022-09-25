package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerASyncWithKey {


    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncWithKey.class.getName());

    public static void main(String[] args) {

        String topicName = "multipart-topic";
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
        for (int seq = 0; seq < 20; seq++) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world" + seq);

        //KafkaProducer send message
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if (Objects.isNull(exception)) {
                logger.info("\n ##### record metadata received ###### \n" +
                        "partition:" + recordMetadata.partition() + "\n" +
                        "offset:" + recordMetadata.offset() + "\n" +
                        "timestamp:" + recordMetadata.timestamp());
            } else {
                logger.error("Exception error from broker" + exception.getMessage());
            }
        });
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
