package com.ing.kafka.reactor.service;

import mysqlcdc.test.RawTransaction.Envelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface TransactionService {

    void process(Envelope rawTransaction);
    void process(List<ConsumerRecord<String, Envelope>> records);


}
