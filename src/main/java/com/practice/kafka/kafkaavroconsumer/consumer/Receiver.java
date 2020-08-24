package com.practice.kafka.kafkaavroconsumer.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    @KafkaListener(topics = "#{'${kafka.topic.avro}'}", groupId = "#{'${kafka.topic.group.id}'}", autoStartup = "#{'${kafka.autoStartup.control}'}", containerFactory = "dltPracticeContainerFactory")
    public void consume(ConsumerRecord<String, GenericRecord> consumerRecord, Acknowledgment acknowledgment) throws IOException {
        //LOGGER.info("Total Records received: {}", consumerRecords.count());
        //for (ConsumerRecord<String, GenericRecord> consumerRecord: consumerRecords) {
            final GenericRecord record = consumerRecord.value();
            if(record.get("favorite_color").equals("Red")) {
                throw new IOException("It seems to be incorrect favorite color");
            }
            LOGGER.info("Received Record Key: {}", consumerRecord.key());
            LOGGER.info("Received Record: {}", record.toString());
        //}
        acknowledgment.acknowledge();
    }
}
