package com.ing.kafka.reactor.service;

import com.ing.kafka.reactor.model.RawTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface TransactionService {

    void process(RawTransaction rawTransaction);
    void process(List<ConsumerRecord<String, RawTransaction>> records);


}
