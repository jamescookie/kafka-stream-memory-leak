package com.example;

import io.micronaut.runtime.Micronaut;

public class Application {
    public static final String TOPIC_NAME = "test-topic";

    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }
}