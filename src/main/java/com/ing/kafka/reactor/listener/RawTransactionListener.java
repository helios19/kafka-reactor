package com.ing.kafka.reactor.listener;

import com.ing.kafka.reactor.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import mysqlcdc.test.RawTransaction.Envelope;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.List;


@Slf4j
@Component
public class RawTransactionListener {

    private KafkaReceiver kafkaReceiver;
    private TransactionService transactionService;

    public RawTransactionListener(TransactionService transactionService, KafkaReceiver kafkaReceiver) {
        this.transactionService = transactionService;
        this.kafkaReceiver = kafkaReceiver;
    }

//    @KafkaListener(topics = "${topics.raw-transaction-data}", containerFactory = "transactionListenerFactory")
//    @Timed
//    public void handleEvents(List<ConsumerRecord<String, RawTransaction>> records, Acknowledgment acknowledgment) {
////            ConsumerRecord<String, RawTransaction> record, Acknowledgment acks) {
////        log.info("received: key={}, value={}", record.key(), record.value());
//
//        // convert avro data to important data
////        RawTransaction sampleData = record.value();
////        ImportantData data = new ImportantData();
////        data.setId(sampleData.getId());
////        data.setName(sampleData.getName());
////        data.setDescription("this is avro data");
//
//        transactionService.process(records);
//
//        acknowledgment.acknowledge();
//
//        log.info("message acknowledged.");
//    }



//    @KafkaListener(topics = "${topics.raw-transaction-data}", containerFactory = "transactionListenerFactory", groupId = "${spring.kafka.consumer.client-id}"/*id="test", topics = "foo.create"*/)
    public void listen(List<Envelope> envelopes, Acknowledgment acknowledgment) {

        log.info("------------------");
        log.info("envelopes.size() : " + envelopes.size());
        log.info("------------------");

        acknowledgment.acknowledge();
    }




//    @KafkaListener(topics = "${topics.raw-transaction-data}", containerFactory = "transactionListenerFactory", groupId = "${spring.kafka.consumer.client-id}")
    public void handler(Envelope envelope) {

        log.info("------------------");
        log.info("in handler --- method");
        log.info("envelope.getAfter().getName() : " + envelope.getAfter().getName());
        log.info("envelope.getAfter().getId() : " + envelope.getAfter().getId());
        log.info("------------------");

    }


////    @KafkaListener(topics = "${topics.raw-transaction-data}", containerFactory = "transactionListenerFactory")
//    public void receiver() {
//
//        log.info("------------------");
//        log.info("in receiver --- method");
//        log.info("------------------");
//
//
//        try {
////            Flux<ReceiverRecord<String,Envelope>> kafkaFlux = kafkaReceiver.receive();
//
//            Flux receiver = kafkaReceiver.receive()
//                    .log()
//                    .doOnNext(o -> {
//                    log.info("o : " + o.getClass().getName());
//            });
////                    .doOnError(throwable -> log.error("throwable : " + throwable.getMessage(), throwable))
////                    .subscribe();
//
//            receiver.subscribeWith(EmitterProcessor.create(false));
//
//
//
////            kafkaFlux.log().toIterable().forEach(receiverRecord -> System.out.println("receiverRecord.value :" + receiverRecord.value().getAfter().getName()));
//        } catch (Exception e) {
//            log.info("===========================");
//            log.error("exception : ", e);
//            log.info("===========================");
//        }
//
//
//
////        kafkaFlux
////                .doOnSubscribe(s -> log.info("doOnSubscribe to display envelope : " + s.toString()))
////                .doOnNext(record -> {
////                    try {
////
////                        record.receiverOffset().commit()
////                                .doOnError(e -> log.error("error", e))
////                                .doOnSuccess(i -> transactionService.process(record.value()))
//////                                .retry(retryPredicate)
////                                .subscribe();
////                    } catch (Exception e) {
////                        log.error("Unexpected exception", e);
////                    }
////                })
////                .doOnError(e -> log.error("KafkaFlux exception", e));
//
//
//
//
//
////        kafkaFlux
//////                .log()
////                .doOnNext(r -> r.receiverOffset().acknowledge())
////                .map(ReceiverRecord::value)
//////                .log()
////                .doOnNext(subscription -> transactionService.process((Envelope) subscription))
//////                {
//////                    System.out.println("record : " + (Envelope) subscription);
//////                    transactionService.process((Envelope) subscription);
//////                })
////                .doOnError(e -> System.err.println(e))
////                .subscribe();
//
//
//
//
//
//
//
//
////                .doOnNext(envelope -> log.info("subscription record : " + envelope.getAfter().getName()));
//
////                .doOnSubscribe(subscription -> System.out.println("subscription record : " + ((Envelope) subscription).getAfter().getName()))
////                .subscribe();
////                .bufferUntil(envelope -> envelope.getBefore().getName() != null);
//
//
////        kafkaReceiver.receive()
////                .log()
////                .bufferUntil(o -> {
////                    System.out.println("Records in while buffering : " + o.toString());
////                    return true;
////
////                })
////                .map(r -> ((FluxOperator) r).toIterable())
////                .doOnSubscribe(records ->
////                        System.out.println("record : " + (MapSubscriber) records))
//////                        transactionService.process((List<ConsumerRecord<String, Envelope>>) records))
////                .log()
////                .doOnError(e -> System.err.println(e))
////                .subscribe(System.out::println);
//
//
////        kafkaDataReceiver.receive()
////                .map { it.value() }
////        .share()
////                .log()
//
//
////        receiver.receive()
////                .doOnNext(r -> {
////                    process(r);
////                    r.receiverOffset().commit().block();
////                });
//
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