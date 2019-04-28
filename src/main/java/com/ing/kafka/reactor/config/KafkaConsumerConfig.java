package com.ing.kafka.reactor.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.ing.kafka.reactor.converter.SimpleSchemaMessageConverter;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import mysqlcdc.test.RawTransaction.Envelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.RecordMessageConverter;
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
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> headersListenerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory("headers", String.class));
//        return factory;
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> partitionsListenerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory("partitions", String.class));
//        return factory;
//    }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> filterListenerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory("filter", String.class));
//        factory.setRecordFilterStrategy(record -> record.value()
//                .contains("World"));
//        return factory;
//    }





//    public <T extends SpecificRecordBase> ConsumerFactory<String, T> consumerFactory(String groupId, Class<T> clazz) {
//        // build base consumer props from application.yml
//        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
//
//        // add on props specific to the avro consumer
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//
//        System.out.println("-----------------------------");
//        System.out.println("in consumerFactory method");
//        System.out.println("props :" + props);
//        System.out.println("-----------------------------");
//
//
//
////        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
////                new AvroDeserializer<>(User.class));
//
////        return new DefaultKafkaConsumerFactory<>(props);
//
//        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
////                new KafkaAvroDeserializer());
//                new AvroDeserializer<>(clazz));
//    }

//    @Bean
    public ConsumerFactory<String, Envelope> kafkaConsumerFactory() {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
//        consumerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        consumerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

//        // Schema Registry
//        consumerProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
        return new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    @Bean("transactionListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Envelope> transactionListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Envelope> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.getContainerProperties().setAckMode(MANUAL);
//        factory.setConsumerFactory(consumerFactory("rawTransaction-listener", RawTransaction.class));
        factory.setConsumerFactory(kafkaConsumerFactory());

//        factory.setMessageConverter(new StringJsonMessageConverter());

//        factory.setBatchListener(true);
//        factory.setMessageConverter(new BatchMessagingMessageConverter(converter()));

        factory.setMessageConverter(converter());

        factory.setRetryTemplate(retryTemplate);

        // create simple recovery callback (this gets executed once retry policy limits have been exceeded)
        factory.setRecoveryCallback(context -> {
            log.error("RetryPolicy limit has been exceeded!", context.getLastThrowable());
            return null;
        });


        // Error handler
//        factory.setErrorHandler(new SeekToCurrentErrorHandler());


        // Filter
//        ConcurrentKafkaListenerContainerFactory<String, String> factory
//                = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setRecordFilterStrategy(
//                record -> record.value().contains("World"));
//        return factory;

        return factory;
    }



    @Bean
    Map consumerProps() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        // add on props specific to the avro consumer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.12.12.24:9092,192.14.14.28:9092");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "example-group");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,AvroDeserializer.class);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        log.info("-----------------------------");
        log.info("in consumerProps method");
        log.info("props :" + props);
        log.info("-----------------------------");

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















    @Bean
    public RecordMessageConverter converter() {
        return new SimpleSchemaMessageConverter();
//        return new AvroSchemaMessageConverter(avroMapper(), schemaRegistry());
    }

    @Bean
    public SchemaRegistry schemaRegistry() {
        return new SchemaRegistry();
    }

    @Bean
    public AvroMapper avroMapper() {
        AvroMapper mapper = new AvroMapper();
        mapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
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


//@Component
//@KafkaListener(id = "multiGroup", topics = { "foos", "bars" })
//public class MultiMethods {
//
//    @KafkaHandler
//    public void foo(Foo1 foo) {
//        System.out.println("Received: " + foo);
//    }
//
//    @KafkaHandler
//    public void bar(Bar bar) {
//        System.out.println("Received: " + bar);
//    }
//
//    @KafkaHandler(isDefault = true)
//    public void unknown(Object object) {
//        System.out.println("Received unknown: " + object);
//    }
//
//}