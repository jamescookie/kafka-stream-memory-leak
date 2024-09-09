package com.example.kafka;

import com.example.Application;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

@Factory
public class KafkaStreamsFactory {
    private static final String TABLE_NAME = "my-global-table";

    @Property(name = "micronaut.application.name")
    private String applicationName;
    @Property(name = "kafka.bootstrap.servers")
    private String bootstrapServers;

    @Singleton
    public ReadOnlyKeyValueStore<String, String> store() {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, applicationName);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.start();

        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(TABLE_NAME, QueryableStoreTypes.keyValueStore()));
    }

    private static Topology buildTopology() {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        builder.globalTable(
                Application.TOPIC_NAME,
                Consumed.with(stringSerde, stringSerde),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME).withStoreType(Materialized.StoreType.IN_MEMORY));

        return builder.build();
    }
}
