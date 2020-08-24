package com.practice.kafka.kafkaavroconsumer.config;

import com.practice.kafka.kafkaavroconsumer.consumer.Receiver;
import com.practice.kafka.kafkaavroconsumer.consumer.ReceiverDLT;
import com.practice.kafka.kafkaavroconsumer.error.handler.GlobalErrorHandler;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private KafkaTemplate<Object, Object> template;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://127.0.0.1:8081");
        props.put("specific.avro.reader", true);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        return props;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setErrorHandler(new SeekToCurrentErrorHandler());
        //factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        //factory.setBatchListener(true);
        //factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        return factory;
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG,"10");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://127.0.0.1:8081");

        return props;
    }

    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<Object, Object>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        this.template = new KafkaTemplate<Object, Object>(producerFactory());
        return this.template;
    }

    @Bean
    public Receiver receiver() {
        return new Receiver();
    }

    @Bean
    public ReceiverDLT receiverDLT() {
        return new ReceiverDLT();
    }

    @Bean(name = "dltPracticeContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> dltPracticeContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(template,
                ((consumerRecord, e) -> new TopicPartition("user-test-topic.DLT", consumerRecord.partition()))), 3));
        factory.getContainerProperties().setAckOnError(false);

        return factory;
    }
}
