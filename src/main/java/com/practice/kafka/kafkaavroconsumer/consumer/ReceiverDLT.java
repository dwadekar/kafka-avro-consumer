package com.practice.kafka.kafkaavroconsumer.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.Acknowledgment;

public class ReceiverDLT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverDLT.class);

    @KafkaListener(topics = "#{'${kafka.topic.avro.dlt}'}", groupId = "cg-user-DLT121", autoStartup = "#{'${kafka.autoStartup.control}'}")
    public void consume(ConsumerRecord<String, GenericRecord> consumerRecord, Acknowledgment acknowledgment){
        //LOGGER.info("Total DLT Records received: {}", consumerRecords.count());
        //for (ConsumerRecord<String, GenericRecord> consumerRecord: consumerRecords) {
            final GenericRecord record = consumerRecord.value();
            LOGGER.info("Received DLT Record Key: {}", consumerRecord.key());
            LOGGER.info("Received DLT Record: {}", record.toString());
        //}
        acknowledgment.acknowledge();
    }

}
