package com.test.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

import static com.test.kafka.ConsumerUtil.SIMPLE_LINE;

/**
 * Consumer class responsible for checking different scenarios on interacting with a kafka broker.
 *
 * @author Sajad
 */
public class KafKaConsumer {

    /**
     * Logger
     */
    final static Logger logger = LoggerFactory.getLogger(KafKaConsumer.class);
    /**
     * String constant indicating the test is passed.
     */
    private static final String PASSED_TEST = "TEST-PASSED";
    /**
     * String constant indicating the test is failed.
     */
    private static final String FAILED_TEST = "TEST-FAILED";
    /**
     * Provided topic name
     */
    private static String CUSTOM_TOPIC = null;
    /**
     * Provided bootstrap server address
     */
    private static String BOOTSTRAP_SERVER = null;

    /**
     * The main method, first checks the input provided by arguments and if not found, will check the properties file.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) throws Exception {
        Configuration configurationPair;

        if (args.length == 4) {
            try {
                configurationPair = ConsumerUtil.resolveCommandLineArguments(args);
            } catch (IllegalArgumentException e) {
                logger.error("Illegal command line arguments provided.");
                return;
            }
        } else
            try {
                configurationPair = ConsumerUtil.resolvePropertiesFile();
            } catch (IllegalArgumentException e) {
                logger.error("Illegal config file provided.");
                return;
            }

        // Set bootstrap address and topic
        BOOTSTRAP_SERVER = configurationPair.getAddress();
        CUSTOM_TOPIC = configurationPair.getTopic();

        // Display THE pre-run message
        logger.warn(SIMPLE_LINE);
        logger.info(configurationPair.getMessage());

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
        }
    }

    /**
     * Tries to validate successful broker establishment.
     */
    public static void checkValidConnection() {
        logger.warn(SIMPLE_LINE);
        logger.info("About to connect to a valid broker, should connect within 5 seconds...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER, CUSTOM_TOPIC);
        Callable<Map> run = consumer::listTopics;
        RunnableFuture<Map> future = new FutureTask<>(run);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(future);
        try {
            future.get(2, TimeUnit.SECONDS);   // wait 5 seconds, it should connect
            logger.info(PASSED_TEST);
        } catch (Exception ex) {
            // timed out. Try to stop the code if possible.
            future.cancel(true);
            logger.info(FAILED_TEST);
        }
        service.shutdown();
        consumer.close();
    }

    /**
     * Tries to validate failed connection establishment due to wrong address.
     */
    private static void checkInvalidConnection() {
        logger.warn(SIMPLE_LINE);
        logger.info("Test invalid connection, fail fast if it took longer than 5 seconds...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER, CUSTOM_TOPIC);
        Callable<Map> run = consumer::listTopics;
        RunnableFuture<Map> future = new FutureTask<>(run);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(future);
        try {
            future.get(5, TimeUnit.SECONDS);   // wait 5 seconds
            logger.info(FAILED_TEST);
        } catch (Exception ex) {
            // timed out. Try to stop the code if possible.
            future.cancel(true);
            logger.info(PASSED_TEST);
        }
        service.shutdown();
        consumer.close();
    }

    /**
     * Tries to validate broker establishment and topic existence.
     */
    private static void checkValidTopic() {
        logger.warn(SIMPLE_LINE);
        logger.info("Test valid topic discovery...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER, CUSTOM_TOPIC);
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
            consumer.unsubscribe();
            consumer.close();
        }
    }

    /**
     * The assumption is that there is already at least a producer which producing messages to the defined topic.
     */
    private static void checkConsumeCurrentMessages() {
        logger.warn(SIMPLE_LINE);
        logger.info("About to open a connection for 10 seconds to a valid broker and consume current messages...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER, CUSTOM_TOPIC);

        // Wait 10 seconds and consume current messages
        var consumedSomeMessages = false;
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            // Assert records count
            consumedSomeMessages = consumerRecords.count() > 0;

            consumer.commitAsync();
        }

        if (consumedSomeMessages)
            logger.info(PASSED_TEST);
        else
            logger.info(FAILED_TEST);

        consumer.close();
    }

    /**
     * Keep the connection open for 60 seconds and listen to the container's default topic and assert the messages count
     * and birth time.
     *
     * @throws ExecutionException   Execution exception has thrown.
     * @throws InterruptedException Interrupted exception has thrown.
     */
    private static void checkConsumeDummyMessages() throws ExecutionException, InterruptedException {
        logger.warn(SIMPLE_LINE);
        logger.info("Test to open a connection for 10 seconds to a valid broker and consume dummy produced messages...");
        final var dummyMessageCount = 50;
        ProducerUtil.runProducer(BOOTSTRAP_SERVER, dummyMessageCount, CUSTOM_TOPIC);
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER, CUSTOM_TOPIC);

        // Wait 10 seconds
        var startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            // Poll for new messages and keep alive the consumer
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            if (consumerRecords.count() == dummyMessageCount) {
                logger.info("Consumer message count = produced messages");

                /*
                Check messages are recently produced
                If the message is produced older than 1 second ago, then it's not out our produced message
                 */
                consumerRecords.forEach(message -> {
                    if (startTime - message.key() > 1000)
                        logger.info(FAILED_TEST);
                });
                logger.info(PASSED_TEST);
            }
            consumer.commitAsync();
        }
        consumer.close();
    }

    protected enum TestsToRun {
        VALID_CONNECTION, INVALID_CONNECTION, VALID_TOPIC, CONSUME_CURRENT_MESSAGES, CONSUME_DUMMY_MESSAGES
    }
}
