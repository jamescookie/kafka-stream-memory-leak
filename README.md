## Memory leak in Kafka Streams 3.8.0

This is a simple micronaut application (built via micronaut launch) to add a framework so that it is easy to see the issue.
It will also fail without using any framework, but there would be more boilerplate code that would make it harder to see the key parts.

### Usage

Run `./gradlew clean build` and the `shouldNotRunOutOfMemory` will run out of memory. You can attach VisualVM (or the like) to see the memory grow.

### Revert

Revert to `org.apache.kafka:kafka-streams:3.7.1` and the problem does not happen

### Workaround

Use a try-with-resources statement around the `store.all()` to fix the issue (see endpoint `/messages/count/working`)

