package com.test.kafka;

public class Configuration {
    private final String address;
    private final String topic;
    private final String message;
    private final KafKaConsumer.TestToRun testToRun;

    public Configuration(String host, String topic, String t, KafKaConsumer.TestToRun u) {
        this.address = host;
        this.topic = topic;
        this.message = t;
        this.testToRun = u;
    }

    public String getMessage() {
        return message;
    }

    public KafKaConsumer.TestToRun getTestToRun() {
        return testToRun;
    }

    public String getAddress() {
        return address;
    }

    public String getTopic() {
        return topic;
    }
}