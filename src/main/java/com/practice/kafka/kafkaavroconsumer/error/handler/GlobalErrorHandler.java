package com.practice.kafka.kafkaavroconsumer.error.handler;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareBatchErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;

public class GlobalErrorHandler implements ConsumerAwareErrorHandler /*ConsumerAwareBatchErrorHandler*/ {

    private static final Logger log = LoggerFactory.getLogger(GlobalErrorHandler.class);

    /*@Override
    public void handle(Exception e, ConsumerRecords<?, ?> consumerRecords, Consumer<?, ?> consumer) {
        for (ConsumerRecord<?, ?> consumerRecord: consumerRecords) {
            log.warn("Global error handler for message: {}, with exception as: {}", consumerRecord.value().toString(), e.getMessage());
        }

    }*/

    @Override
    public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord, Consumer<?, ?> consumer) {
        log.warn("Global error handler for message: {}, with exception as: {}", consumerRecord.value().toString(), e.getMessage());
    }
}
