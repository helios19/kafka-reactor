package com.ing.kafka.reactor.service;

import mysqlcdc.test.RawTransaction.Envelope;

import java.util.List;

public interface TransactionService {

    void process(Envelope rawTransaction);
    void process(List<Envelope> records);


}
