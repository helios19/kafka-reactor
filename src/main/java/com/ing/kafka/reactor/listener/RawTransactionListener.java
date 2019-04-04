package com.ing.kafka.reactor.listener;

import com.ing.kafka.reactor.model.RawTransaction;
import com.ing.kafka.reactor.service.TransactionService;
import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.List;
import java.util.UUID;


@Slf4j
@Component
public class RawTransactionListener {

    private KafkaReceiver kafkaReceiver;
    private TransactionService transactionService;

    public RawTransactionListener(TransactionService transactionService, KafkaReceiver kafkaReceiver) {
        this.transactionService = transactionService;
        this.kafkaReceiver = kafkaReceiver;
    }

    @KafkaListener(topics = "${topics.raw-transaction-topic}", containerFactory = "transactionListenerFactory")
    @Timed
    public void handleEvents(List<ConsumerRecord<String, RawTransaction>> records, Acknowledgment acknowledgment) {
//            ConsumerRecord<String, RawTransaction> record, Acknowledgment acks) {
//        log.info("received: key={}, value={}", record.key(), record.value());

        // convert avro data to important data
//        RawTransaction sampleData = record.value();
//        ImportantData data = new ImportantData();
//        data.setId(sampleData.getId());
//        data.setName(sampleData.getName());
//        data.setDescription("this is avro data");

        transactionService.process(records);

        acknowledgment.acknowledge();

        log.info("message acknowledged.");
    }





    @KafkaListener(topics="hellochange",groupId="example-group")
    public void receiver() {
        kafkaReceiver.receive().bufferUntil(o -> {true})
        .subscribe(System.out::println);




//        receiver.receive()
//                .doOnNext(r -> {
//                    process(r);
//                    r.receiverOffset().commit().block();
//                });

    }




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





//    private final EmitterProcessor<ServerSentEvent<String>> emitter = EmitterProcessor.create();
//
//    public Flux<ServerSentEvent<String>> get()
//    {
//        return emitter.log();
//    }
//
//    @KafkaListener(topics = "${kafka.topic.name}")
//    public void receive(String data)
//    {
//        emitter.onNext(ServerSentEvent.builder(data).id(UUID.randomUUID().toString()).build());
//    }