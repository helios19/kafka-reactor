package com.ing.kafka.reactor.listener;

import com.ing.kafka.reactor.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import mysqlcdc.test.RawTransaction.Envelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryListener {
    private final TransactionService transactionService;
    public RetryListener(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

//    @KafkaListener(topics = "${topics.retry-data}",clientIdPrefix = "kafka-samples-client-bis", containerFactory = "transactionListenerFactory")
    public void listen(ConsumerRecord<String, Envelope> record, Acknowledgment acks) {
        log.info("received: key={}, value={}", record.key(), record.value());
        transactionService.process(record.value());
        acks.acknowledge();
        log.info("message acknowledged.");
    }
}
