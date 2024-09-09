package com.example.controller;

import com.example.kafka.KafkaProducer;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import jakarta.inject.Inject;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.example.Application.TOPIC_NAME;

@Controller
public class SimpleController {
    @Inject
    private ReadOnlyKeyValueStore<String, String> store;

    @Inject
    private KafkaProducer kafkaProducer;

    @Get(value = "/messages/create/{numberToCreate}", produces = MediaType.TEXT_PLAIN) // This should be a post, but to keep it simple while testing
    public String createMessages(@PathVariable("numberToCreate") Integer numberToCreate) {
        Map<String, String> messages = new HashMap<>();
        for (int i = 0; i < numberToCreate; i++) {
            messages.put(UUID.randomUUID().toString(), "Message " + i);
        }
        new Thread(() -> kafkaProducer.send(TOPIC_NAME, messages)).start();
        return "Creating " + numberToCreate;
    }

    @Get(value = "/messages/count/memory-leak", produces = MediaType.TEXT_PLAIN)
    public int countMessages() {
        AtomicInteger count = new AtomicInteger();
        store.all().forEachRemaining(kv -> count.getAndIncrement());  // Problem is here, calling .all() on the store
        return count.get();
    }

    @Get(value = "/messages/count/working", produces = MediaType.TEXT_PLAIN)
    public int countMessagesWorking() {
        AtomicInteger count = new AtomicInteger();
        try (var all = store.all()) { // fix is to close the KeyValueIterator
            all.forEachRemaining(kv -> count.getAndIncrement());
        }
        return count.get();
    }
}
