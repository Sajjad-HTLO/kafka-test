import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;
import java.util.concurrent.*;

public class ConsumerTest {

    private static String DEFAULT_VALID_TOPIC = "my-topic";
    private static final String INVALID_TOPIC = "invalid-topic";
    private static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String WRONG_BOOTSTRAP_SERVER = "localhost:9066";

    public static void main(String[] args) throws Exception {
        StringBuilder message = new StringBuilder();
        final TestsToRun testsToRun;
        String address = null;
        String customTopic = null;

        if (args.length > 3) {
            String host = args[0];
            String port = args[1];
            customTopic = args[2];
            String testMethod = args[3];

            message.append("Run the app on host: " + host + ", port: " + port + ", topic: " + customTopic);

            address = host.concat(":").concat(port);

            if (testMethod.equalsIgnoreCase("vc")) {
                message.append(", Test connection.");
                testsToRun = TestsToRun.VALID_CONNECTION;
            } else if (testMethod.equalsIgnoreCase("ic")) {
                message.append(", Test invalid connection.");
                testsToRun = TestsToRun.INVALID_CONNECTION;
            } else if (testMethod.equalsIgnoreCase("vt")) {
                message.append(", Test valid topic discovery.");
                testsToRun = TestsToRun.VALID_TOPIC;
            } else if (testMethod.equalsIgnoreCase("it")) {
                message.append(", Test invalid topic discovery.");
                testsToRun = TestsToRun.INVALID_TOPIC;
            } else if (testMethod.equalsIgnoreCase("all")) {
                message.append(", All test cases.");
                testsToRun = TestsToRun.ALL_TESTS;
            } else {
                message.append(", Wrong test method provided, defaulting to all test cases..");
                testsToRun = TestsToRun.ALL_TESTS;
            }
        } else {
            message.append("Arguments are not provided, or not correct, defaulting to " + DEFAULT_BOOTSTRAP_SERVER + ", topic: " + DEFAULT_VALID_TOPIC + "" +
                    " and running all test cases");
            testsToRun = TestsToRun.ALL_TESTS;
        }

        if (customTopic != null)
            DEFAULT_VALID_TOPIC = customTopic;

        // Display pre-run message
        System.out.println(message);

        final String resolvedAddress = address != null ? address : DEFAULT_BOOTSTRAP_SERVER;

        switch (testsToRun) {
            case VALID_CONNECTION:
                testSuccessfulConnection(resolvedAddress);
                break;
            case INVALID_CONNECTION:
                testInvalidConnection(resolvedAddress);
                break;
            case VALID_TOPIC:
                testValidTopic(resolvedAddress);
                break;
            case INVALID_TOPIC:
                testInvalidTopic(resolvedAddress);
                break;
            case ALL_TESTS:
                testSuccessfulConnection(resolvedAddress);
                testInvalidConnection(resolvedAddress);
                testConsumeCurrentMessages(resolvedAddress);
                testConsumeDummyMessages(resolvedAddress);
                break;
        }
    }

    private static void testInvalidConnection(String bootStrapServer) {
        System.out.println("Test invalid broker connection, fail if it took longer than 20 seconds...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(bootStrapServer, DEFAULT_VALID_TOPIC);

        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<Object> task = consumer::listTopics;
        Future<Object> future = executor.submit(task);

        // If it took longer than 10 seconds, then the connection establishment is considered as failed.
        try {
            future.get(10, TimeUnit.SECONDS);
            System.out.println();
        } catch (TimeoutException ex) {
            System.out.println("TEST FAILED.");
        } catch (InterruptedException e) {
            System.out.println("TEST FAILED.");
        } catch (ExecutionException e) {
            System.out.println("TEST FAILED.");
        } catch (java.util.concurrent.TimeoutException e) {
            System.out.println("TEST SUCCESSFUL");
        }
    }

    private static void testSuccessfulConnection(String bootStrapServer) {
        System.out.println("About to connect to a valid broker...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(bootStrapServer, DEFAULT_VALID_TOPIC);
        try {
            // Call to list topics, we'll get timeout exception if the connection was wrong.
            consumer.listTopics();
            System.out.println("TEST SUCCESSFUL.");
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to broker.");
            System.out.println("TEST FAILED.");
        }
    }

    private static void testValidTopic(String bootStrapServer) {
        System.out.println("Test valid topic discovery...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(bootStrapServer, DEFAULT_VALID_TOPIC);
        try {
            System.out.println("Topic length should be > 0: " + (consumer.listTopics().size() > 0));
            // Validate that our topic is among them
            System.out.println("Can discover our topic: " + consumer.listTopics().keySet().contains(DEFAULT_VALID_TOPIC));
            System.out.println("TEST SUCCESSFUL.");
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to broker.");
            System.out.println("TEST FAILED.");
        }
    }

    private static void testInvalidTopic(String bootStrapServer) {
        System.out.println("Test invalid topic discovery...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(bootStrapServer, INVALID_TOPIC);
        try {
            System.out.println("Topic length should be > 0: " + (consumer.listTopics().size() > 0));
            // Validate that our topic is among them
            System.out.println("Can discover our topic: " + consumer.listTopics().keySet().contains(DEFAULT_VALID_TOPIC));
            System.out.println("TEST SUCCESSFUL.");
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to broker.");
            System.out.println("TEST FAILED.");
        }
    }

    private static void testConsumeCurrentMessages(String bootStrapServer) {
        System.out.println("About to open a connection for 10 seconds to a valid broker and consume some messages...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(bootStrapServer, DEFAULT_VALID_TOPIC);

        // Wait 10 seconds
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            // Consumer poll every one second
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            int recordsCount = consumerRecords.count();
            System.out.println("consumer records count: " + recordsCount);

            if (recordsCount > 0)
                System.out.println("TEST SUCCESSFUL.");

            consumer.commitAsync();
        }

        consumer.close();

    }

    private static void testConsumeDummyMessages(String bootStrapServer) throws ExecutionException, InterruptedException {
        System.out.println("Test to open a connection for 10 seconds to a valid broker and consume dummy messages...");
        final var dummyMessageCount = 50;
        ProducerUtil.runProducer(dummyMessageCount);
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(bootStrapServer, DEFAULT_VALID_TOPIC);

        // Wait 10 seconds
        var startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            // Poll for new messages and keep alive the consumer
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            if (consumerRecords.count() > 0)
                System.out.println("Consumer message count = produced messages: " + (consumerRecords.count() == dummyMessageCount));

            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("TEST SUCCESSFUL.");
    }

    private enum TestsToRun {
        ALL_TESTS, VALID_CONNECTION, INVALID_CONNECTION, VALID_TOPIC, INVALID_TOPIC
    }
}
