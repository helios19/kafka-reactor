package com.ing.kafka.reactor.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import static java.util.Collections.singletonMap;

@Slf4j
@Configuration
public class RetryConfig {

    private final KafkaProperties kafkaProperties;

    public RetryConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // retry 5 times, but only for KafkaException
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(5, singletonMap(KafkaException.class, true)));

        // fixed backoff
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(50); // milliseconds

        retryTemplate.setBackOffPolicy(policy);

        return retryTemplate;
    }

}
