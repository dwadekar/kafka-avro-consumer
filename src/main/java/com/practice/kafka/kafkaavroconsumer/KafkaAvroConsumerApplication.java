package com.practice.kafka.kafkaavroconsumer;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Log4j2
@SpringBootApplication
public class KafkaAvroConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaAvroConsumerApplication.class, args);
	}

	@Bean
	public ApplicationRunner runner(KafkaAdmin admin) {
		return args -> {
			try (AdminClient client = AdminClient.create(admin.getConfig())) {
				while (true) {
					Map<String, ConsumerGroupDescription> map =
							client.describeConsumerGroups(Collections.singletonList("3423429")).all().get(10, TimeUnit.SECONDS);
					System.out.println(map.toString());
					ConsumerGroupDescription grpDesc = map.get("3423429");
					System.out.println("Here is group Desc: " + grpDesc.state().name());

					System.in.read();
				}
			}
		};
	}

}
