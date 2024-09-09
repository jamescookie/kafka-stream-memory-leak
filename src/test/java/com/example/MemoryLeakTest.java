package com.example;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.example.Application.TOPIC_NAME;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MemoryLeakTest implements TestPropertyProvider {
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("7.6.1"));

    @Inject
    private EmbeddedApplication<?> application;

    @Inject
    @Client("/")
    private HttpClient client;

    @BeforeAll
    static void beforeAll() {
        createTopics(kafkaContainer.getFirstMappedPort(), TOPIC_NAME);
    }

    @AfterAll
    static void afterAll() {
        kafkaContainer.stop();
    }

    @Override
    public Map<String, String> getProperties() {
        kafkaContainer.start();
        return Map.of("kafka.bootstrap.servers", kafkaContainer.getBootstrapServers());
    }

    @Test
    void applicationStarts() {
        Assertions.assertTrue(application.isRunning());
    }

    @Test
    void shouldNotRunOutOfMemory() {
        int numberOfMessages = 50000;

        Assertions.assertEquals(200, client.toBlocking().exchange(HttpRequest.GET("/messages/create/" + numberOfMessages), String.class).code());

        await().forever().pollInterval(1, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() ->
                Assertions.assertEquals(
                        numberOfMessages + 1, // This deliberately never passes, it just sits polling until we run out of memory
                        client.toBlocking().exchange(HttpRequest.GET("/messages/count/memory-leak"), Integer.class).body()
                )
        );
    }

    private static void createTopics(Integer mappedPort, String... topics) {
        var newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, 1, (short) 1))
                        .collect(Collectors.toList());
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d", "localhost", mappedPort)))) {
            admin.createTopics(newTopics);
        }
    }
}
