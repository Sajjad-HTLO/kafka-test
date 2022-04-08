package com.test.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

public class ConsumerUtil {

    protected static final String SIMPLE_LINE = "==============================================================================================";

    /**
     * Resolves the config file contents and instantiates an instance of {@linkplain Configuration}.
     *
     * @return Initialized instance of {@linkplain Configuration}.
     */
    protected static Configuration resolvePropertiesFile() throws IllegalArgumentException {
        try (InputStream input = KafKaConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {

            var prop = new Properties();

            // load a properties file
            prop.load(input);

            if (!prop.isEmpty()) {
                StringBuilder startupMessage = new StringBuilder();
                String host = prop.getProperty("kafka.host");
                String port = prop.getProperty("kafka.port");
                String topic = prop.getProperty("kafka.topic");
                String testMethod = prop.getProperty("kafka.tests_to_run");

                startupMessage.append("About to run the application on ")
                        .append(host).append(":")
                        .append(port)
                        .append(" topic: ")
                        .append(topic);

                // Set custom topic and address
                String address = host.concat(":").concat(port);

                return resolveTestToRun(address, topic, startupMessage, testMethod);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Invalid arguments provided
        throw new IllegalArgumentException();
    }

    /**
     * Resolved the command line arguments and instantiates an instance of {@linkplain Configuration}.
     *
     * @return Initialized instance of {@linkplain Configuration}.
     */
    protected static Configuration resolveCommandLineArguments(String[] args) throws IllegalArgumentException {
        var message = new StringBuilder();
        if (args.length == 4) {
            String host = args[0];
            String port = args[1];
            String topic = args[2];
            String testMethod = args[3];

            message.append("About to run the app on host: " + host + ", port: " + port + ", topic: " + topic);

            String address = host.concat(":").concat(port);

            return resolveTestToRun(address, topic, message, testMethod);
        }

        // Invalid arguments provided
        throw new IllegalArgumentException();
    }

    private static Configuration resolveTestToRun(String bootstrapHost, String topic, StringBuilder message, String testMethod) {
        KafKaConsumer.TestToRun testsToRun;

        if (testMethod.equalsIgnoreCase("vc")) {
            message.append(", test connection.");
            testsToRun = KafKaConsumer.TestToRun.VALID_CONNECTION;
        } else if (testMethod.equalsIgnoreCase("ic")) {
            message.append(", test invalid connection.");
            testsToRun = KafKaConsumer.TestToRun.INVALID_CONNECTION;
        } else if (testMethod.equalsIgnoreCase("ct")) {
            message.append(", test topic discovery.");
            testsToRun = KafKaConsumer.TestToRun.VALID_TOPIC;
        } else if (testMethod.equalsIgnoreCase("ccm")) {
            message.append(", test consume current messages.");
            testsToRun = KafKaConsumer.TestToRun.CONSUME_CURRENT_MESSAGES;
        } else if (testMethod.equalsIgnoreCase("cdm")) {
            message.append(", test consume dummy messages.");
            testsToRun = KafKaConsumer.TestToRun.CONSUME_DUMMY_MESSAGES;
        } else {
            throw new IllegalArgumentException();
        }

        return new Configuration(bootstrapHost, topic, message.toString(), testsToRun);
    }

    static Consumer<Long, String> createConsumer(String serverName, String topicName) {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverName);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topicName));

        return consumer;
    }
}
