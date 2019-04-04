package com.ing.kafka.reactor.service;

import com.ing.kafka.reactor.model.RawTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.util.List;

@Service
public class TransactionServiceImpl implements TransactionService {

    @Override
    public void process(RawTransaction rawTransaction) {

    }

    @Override
    public void process(List<ConsumerRecord<String, RawTransaction>> records) {


        records.stream()


        EmitterProcessor<Person> processor = EmitterProcessor.create();
        BlockingSink<Person> incoming = processor.connectSink();
        inputRecords = KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(m -> incoming.emit(m.value()));

        outputRecords1 = processor.publishOn(scheduler1).map(p -> process1(p));
        outputRecords2 = processor.publishOn(scheduler2).map(p -> process2(p));

        Flux.merge(sender.send(outputRecords1), sender.send(outputRecords2))
                .doOnSubscribe(s -> inputRecords.subscribe())
                .subscribe();





        Scheduler scheduler = Schedulers.newElastic("sample", 60, true);
        KafkaReceiver.create(receiverOptions)
                .receive()
                .groupBy(m -> m.receiverOffset().topicPartition())
                .flatMap(partitionFlux ->
                        partitionFlux.publishOn(scheduler)
                                .map(r -> processRecord(partitionFlux.key(), r))
                                .sample(Duration.ofMillis(5000))
                                .concatMap(offset -> offset.commit()));




    }
}


//    ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton(topic))
//            .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
//            .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
//    Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
//return kafkaFlux.subscribe(record -> {
//        ReceiverOffset offset = record.receiverOffset();
//        System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
//        offset.topicPartition(),
//        offset.offset(),
//        dateFormat.format(new Date(record.timestamp())),
//        record.key(),
//        record.value());
//        offset.acknowledge();
//        latch.countDown();
//        });
