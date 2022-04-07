package com.test.kafka;

public class ConfigurationPair {
    private final String message;
    private final KafKaConsumer.TestsToRun testsToRun;

    public ConfigurationPair(String t, KafKaConsumer.TestsToRun u) {
        this.message = t;
        this.testsToRun = u;
    }

    public String getMessage() {
        return message;
    }

    public KafKaConsumer.TestsToRun getTestsToRun() {
        return testsToRun;
    }
}