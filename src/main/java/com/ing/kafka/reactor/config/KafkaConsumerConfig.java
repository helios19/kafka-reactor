package com.ing.kafka.reactor.config;

import com.ing.kafka.reactor.model.RawTransaction;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.retry.support.RetryTemplate;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Arrays;
import java.util.Map;

import static org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.MANUAL;

/**
 * Kafka Consumer configuration.
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {
    private final KafkaProperties kafkaProperties;
    private final RetryTemplate retryTemplate;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, RetryTemplate retryTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.retryTemplate = retryTemplate;
    }

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> fooListenerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory("foo", String.class));
//        return factory;
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> barListenerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory("bar", String.class));
//        return factory;
//    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> headersListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("headers", String.class));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> partitionsListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("partitions", String.class));
        return factory;
    }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> filterListenerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory("filter", String.class));
//        factory.setRecordFilterStrategy(record -> record.value()
//                .contains("World"));
//        return factory;
//    }

    public <T> ConsumerFactory<String, T> consumerFactory(String groupId, Class<T> clazz) {
        // build base consumer props from application.yml
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();

        // add on props specific to the avro consumer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);


//        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
//                new AvroDeserializer<>(User.class));

        return new DefaultKafkaConsumerFactory<>(props);

//        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
//                new AvroDeserializer<>(clazz));
    }

    @Bean("transactionListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, RawTransaction> transactionListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, RawTransaction> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.getContainerProperties().setAckMode(MANUAL);
        factory.setConsumerFactory(consumerFactory("rawTransaction-listener", RawTransaction.class));


        factory.setRetryTemplate(retryTemplate);

        // create simple recovery callback (this gets executed once retry policy limits have been exceeded)
        factory.setRecoveryCallback(context -> {
            log.error("RetryPolicy limit has been exceeded!");
            return null;
        });


        return factory;
    }



    @Bean
    Map consumerProps() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        // add on props specific to the avro consumer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.12.12.24:9092,192.14.14.28:9092");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "example-group");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,KafkaAvroDeserializer.class);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    ReceiverOptions receiverOptions() {

        ReceiverOptions receiverOptions = ReceiverOptions.create(consumerProps()).subscription(Arrays.asList("mysqlcdc.test.RawTransaction"));

        return receiverOptions;
    }

    @Bean
    public KafkaReceiver kafkaReceiver() {
        log.info("------------------");
        log.info("in kafkaReceiver method");
        log.info("------------------");
        return KafkaReceiver.create(receiverOptions());
    }


//    @Bean
//    public Map<String, Object> consumerConfigs() {
//        return ImmutableMap.<String, Object>builder()
//                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
//                .put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
//                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
//                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class)
//                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//                .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
//                .put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
//                .build();
//    }

}
