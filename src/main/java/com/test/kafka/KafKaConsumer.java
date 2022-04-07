package com.test.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class KafKaConsumer {
    final static Logger logger = LoggerFactory.getLogger(KafKaConsumer.class);

    private static String CUSTOM_TOPIC = null;
    private static String DEFAULT_CONTAINER_TOPIC = "topic69";
    private static String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String PASSED_TEST = "TEST-PASSED";
    private static final String FAILED_TEST = "TEST-FAILED";

    public static void main(String[] args) throws Exception {
        ConfigurationPair configurationPair = resolveCommandLineArguments(args);
        // Display pre-run message
        System.out.println(configurationPair.getMessage());

//        resolvePropertiesFile();

//        checkValidConnection();

        switch (configurationPair.getTestsToRun()) {
            case VALID_CONNECTION:
                checkValidConnection();
                break;
            case INVALID_CONNECTION:
                checkInvalidConnection();
                break;
            case VALID_TOPIC:
                checkValidTopic();
                break;
            case CONSUME_CURRENT_MESSAGES:
                checkConsumeCurrentMessages();
                break;
            case CONSUME_DUMMY_MESSAGES:
                checkConsumeDummyMessages();
                break;
            case ALL_TESTS:
                checkValidConnection();
                checkInvalidConnection();
                checkConsumeCurrentMessages();
                checkConsumeDummyMessages();
                break;
        }
    }

    private static void resolvePropertiesFile() {
        try (InputStream input = KafKaConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {

            var prop = new Properties();

            // load a properties file
            prop.load(input);

            if (!prop.isEmpty()) {
                StringBuilder message = new StringBuilder();
                TestsToRun testsToRun;

                String testMethod = prop.getProperty("kafka.tests_to_run");

                System.out.println("host:" + prop.getProperty("kafka.host"));
                System.out.println("port:" + prop.getProperty("kafka.port"));
                System.out.println("topic:" + prop.getProperty("kafka.topic"));
                System.out.println("tests_to_run:" + testMethod);

                ConfigurationPair ConfigurationPair = resolveMessageAndTestToRun(testMethod);

            }


            // get the property value and print it out

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static ConfigurationPair resolveMessageAndTestToRun(String testMethod) {
        StringBuilder message = new StringBuilder();
        TestsToRun testsToRun;

        if (testMethod.equalsIgnoreCase("vc")) {
            message.append(", Test connection.");
            testsToRun = TestsToRun.VALID_CONNECTION;
        } else if (testMethod.equalsIgnoreCase("ic")) {
            message.append(", Test invalid connection.");
            testsToRun = TestsToRun.INVALID_CONNECTION;
        } else if (testMethod.equalsIgnoreCase("ct")) {
            message.append(", Check topic discovery.");
            testsToRun = TestsToRun.VALID_TOPIC;
        } else if (testMethod.equalsIgnoreCase("ccm")) {
            message.append(", Check consume current messages.");
            testsToRun = TestsToRun.CONSUME_CURRENT_MESSAGES;
        } else if (testMethod.equalsIgnoreCase("cdm")) {
            message.append(", Check consume dummy messages.");
            testsToRun = TestsToRun.CONSUME_DUMMY_MESSAGES;
        } else if (testMethod.equalsIgnoreCase("all")) {
            message.append(", All test cases.");
            testsToRun = TestsToRun.ALL_TESTS;
        } else {
            message.append(", Wrong test method provided, defaulting to all test cases..");
            testsToRun = TestsToRun.ALL_TESTS;
        }

        return new ConfigurationPair(message.toString(), testsToRun);
    }

    private static ConfigurationPair resolveCommandLineArguments(String[] args) {
        StringBuilder message = new StringBuilder();
        if (args.length > 3) {
            String host = args[0];
            String port = args[1];
            String customTopic = args[2];
            String testMethod = args[3];

            message.append("Run the app on host: " + host + ", port: " + port + ", topic: " + customTopic);

            String address = host.concat(":").concat(port);

            // Set custom topic and address
            CUSTOM_TOPIC = customTopic;
            DEFAULT_BOOTSTRAP_SERVER = address;

            return resolveMessageAndTestToRun(testMethod);
        }

        return null;
    }

    private static void checkInvalidConnection() {
        logger.info("Test invalid connection, fail fast if it took longer than 5 seconds...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(DEFAULT_BOOTSTRAP_SERVER, CUSTOM_TOPIC);
        Callable<Map> run = consumer::listTopics;
        RunnableFuture<Map> future = new FutureTask<>(run);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(future);
        try {
            future.get(2, TimeUnit.SECONDS);   // wait 2 seconds
            logger.info(FAILED_TEST);
        } catch (Exception ex) {
            // timed out. Try to stop the code if possible.
            future.cancel(true);
            logger.info(PASSED_TEST);
        }
        service.shutdown();
        consumer.close();
    }

    public static void checkValidConnection() {
        logger.info("About to connect to a valid broker...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(DEFAULT_BOOTSTRAP_SERVER, CUSTOM_TOPIC);
        try {
            // Call to list topics, we'll get timeout exception if the connection was wrong.
            consumer.listTopics();
            logger.info(PASSED_TEST);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            logger.info("Failed to connect to broker.");
            logger.info(FAILED_TEST);
        }
    }

    private static void checkValidTopic() {
        logger.info("Test valid topic discovery...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(DEFAULT_BOOTSTRAP_SERVER, CUSTOM_TOPIC);
        try {
            // Check if custom topic is provided, then check against it
            boolean canDiscover = consumer.listTopics().keySet().contains(CUSTOM_TOPIC);
            logger.info("Can discover the custom topic: " + canDiscover);
            if (canDiscover)
                logger.info(PASSED_TEST);
            else
                logger.info(FAILED_TEST);

        } catch (TimeoutException e) {
            logger.info("Failed to connect to broker.");
            logger.info(FAILED_TEST);
        } finally {
            consumer.close();
        }
    }

    private static void checkConsumeCurrentMessages() {
        logger.info("About to open a connection for 10 seconds to a valid broker and consume some messages...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(DEFAULT_BOOTSTRAP_SERVER, CUSTOM_TOPIC);

        // Wait 10 seconds
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            // Assert records count
            int recordsCount = consumerRecords.count();

            if (recordsCount > 0)
                logger.info(PASSED_TEST);

            consumer.commitAsync();
        }
        consumer.close();

    }

    /**
     * KeeP the connection open for 60 seconds and listen to the container's default topic and assert the messages count.
     *
     * @throws ExecutionException   Execution exception has thrown.
     * @throws InterruptedException Interrupted exception has thrown.
     */
    private static void checkConsumeDummyMessages() throws ExecutionException, InterruptedException {
        logger.info("Test to open a connection for 10 seconds to a valid broker and consume dummy produced messages...");
        final var dummyMessageCount = 50;
        ProducerUtil.runProducer(DEFAULT_BOOTSTRAP_SERVER, dummyMessageCount, DEFAULT_CONTAINER_TOPIC);
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(DEFAULT_BOOTSTRAP_SERVER, DEFAULT_CONTAINER_TOPIC);

        // Wait 10 seconds
        var startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            // Poll for new messages and keep alive the consumer
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            if (consumerRecords.count() == dummyMessageCount) {
                logger.info("Consumer message count = produced messages");
                logger.info(PASSED_TEST);
            }
            consumer.commitAsync();
        }
        consumer.close();
    }

    protected enum TestsToRun {
        ALL_TESTS, VALID_CONNECTION, INVALID_CONNECTION, VALID_TOPIC, CONSUME_CURRENT_MESSAGES, CONSUME_DUMMY_MESSAGES
    }
}
