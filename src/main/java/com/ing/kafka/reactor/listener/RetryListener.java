package com.ing.kafka.reactor.listener;

import com.ing.kafka.reactor.model.RawTransaction;
import com.ing.kafka.reactor.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryListener {
    private final TransactionService transactionService;
    public RetryListener(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @KafkaListener(topics = "${topics.retry-data}", containerFactory = "transactionListenerFactory")
    public void listen(ConsumerRecord<String, RawTransaction> record, Acknowledgment acks) {
        log.info("received: key={}, value={}", record.key(), record.value());
        transactionService.process(record.value());
        acks.acknowledge();
        log.info("message acknowledged.");
    }
}
