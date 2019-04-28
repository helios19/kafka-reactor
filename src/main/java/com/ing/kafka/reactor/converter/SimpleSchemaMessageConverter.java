package com.ing.kafka.reactor.converter;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.ing.kafka.reactor.config.SchemaRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;

import java.lang.reflect.Type;

@Slf4j
public class SimpleSchemaMessageConverter extends MessagingMessageConverter {

    private AvroMapper avroMapper;
    private SchemaRegistry schemaRegistry;
    private KafkaHeaderMapper headerMapper;


//    public SimpleSchemaMessageConverter(AvroMapper avroMapper, SchemaRegistry schemaRegistry) {
//        this.avroMapper = avroMapper;
//        this.schemaRegistry = schemaRegistry;
//        if (JacksonPresent.isJackson2Present()) {
//            this.headerMapper = new DefaultKafkaHeaderMapper();
//        } else {
//            this.headerMapper = new SimpleKafkaHeaderMapper();
//        }
//    }

    @Override
    protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
        ModelMapper modelMapper = new ModelMapper();
        Object value = null;

        try {
            value = modelMapper.map(record.value(), type);
        } catch(Exception ex) {
            log.error("An error occurred while extracting and convert value", ex);
        }

        return value;
    }

    @Override
    public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {

        throw new UnsupportedOperationException(String.format("Conversion operation for message:[{}] from topic:[{}]", message, defaultTopic));

//        MessageHeaders headers = message.getHeaders();
//        Object topicHeader = headers.get(KafkaHeaders.TOPIC);
//        String topic = null;
//        if (topicHeader instanceof byte[]) {
//            topic = new String(((byte[]) topicHeader), StandardCharsets.UTF_8);
//        } else if (topicHeader instanceof String) {
//            topic = (String) topicHeader;
//        } else if (topicHeader == null) {
//            Assert.state(defaultTopic != null, "With no topic header, a defaultTopic is required");
//        } else {
//            throw new IllegalStateException(KafkaHeaders.TOPIC + " must be a String or byte[], not "
//                    + topicHeader.getClass());
//        }
//        String actualTopic = topic == null ? defaultTopic : topic;
//        Integer partition = headers.get(KafkaHeaders.PARTITION_ID, Integer.class);
//        Object key = headers.get(KafkaHeaders.MESSAGE_KEY);
//        Object payload = convertPayload(message, topic);
//        Long timestamp = headers.get(KafkaHeaders.TIMESTAMP, Long.class);
//        Headers recordHeaders = initialRecordHeaders(message);
//        if (this.headerMapper != null) {
//            this.headerMapper.fromHeaders(headers, recordHeaders);
//        }
//        return new ProducerRecord(topic == null ? defaultTopic : topic, partition, timestamp, key,
//                payload,
//                recordHeaders);
    }

//    protected Object convertPayload(Message<?> message, String topic) {
//        try {
//            return avroMapper.writer(schemaRegistry.getAvroSchema(topic))
//                    .writeValueAsBytes(message.getPayload());
//        } catch (JsonProcessingException e) {
//            throw new ConversionException("Failed to convert object to AvroMessage", e);
//        }
//    }
}