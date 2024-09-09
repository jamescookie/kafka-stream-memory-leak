package com.example.kafka;

import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Singleton
public class KafkaProducer {
    @Property(name = "kafka.bootstrap.servers")
    private String bootstrapServers;

    @Singleton
    public void send(String topic, Map<String, String> messages) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        try (final Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props)) {
            for (var es : messages.entrySet()) {
                producer.send(
                        new ProducerRecord<>(topic, es.getKey(), es.getValue()),
                        (event, ex) -> {
                            if (ex != null) {
                                ex.printStackTrace();
                            }
                        });
            }
        }
    }
}
