package com.test.kafka;

public class Configuration {
    private final String address;
    private final String topic;
    private final String message;
    private final KafKaConsumer.TestsToRun testsToRun;

    public Configuration(String host, String topic, String t, KafKaConsumer.TestsToRun u) {
        this.address = host;
        this.topic = topic;
        this.message = t;
        this.testsToRun = u;
    }

    public String getMessage() {
        return message;
    }

    public KafKaConsumer.TestsToRun getTestsToRun() {
        return testsToRun;
    }

    public String getAddress() {
        return address;
    }

    public String getTopic() {
        return topic;
    }
}