package com.practice.kafka.kafkaavroconsumer.custom.actuator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaHealthIndicator extends AbstractHealthIndicator {

    @Value("3423421")
    private String consumerGroupID;

    private KafkaAdmin kafkaAdmin;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfig())) {
                Map<String, ConsumerGroupDescription> map =
                        client.describeConsumerGroups(Collections.singletonList(consumerGroupID)).all().get(10, TimeUnit.SECONDS);
            ConsumerGroupDescription grpDesc = map.get(consumerGroupID);
            if(grpDesc.state().name().equalsIgnoreCase("Stable")) {
                builder.up().withDetail("State", grpDesc.state().name()).withDetail("Group ID", grpDesc.groupId().toString()).build();
            } else {
                builder.down().withDetail("State", grpDesc.state().name()).withDetail("Group ID", grpDesc.groupId().toString()).build();
            }
        } catch (Exception ex) {
            builder.down(ex);
        }
    }
}
