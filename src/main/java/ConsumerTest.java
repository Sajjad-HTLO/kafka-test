import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;

public class ConsumerTest {

    private static final String TOPIC = "my-topic-1";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String WRONG_BOOTSTRAP_SERVER = "localhost:9066";

    public static void main(String[] args) throws Exception {
        StringBuilder message = new StringBuilder();
        final TestsToRun testsToRun;
        if (args.length > 0) {
            message.append("Run the app on host: " + args[0] + ", port: " + args[1]);

            String testMethod = args[2];

            if (testMethod.equalsIgnoreCase("c")) {
                message.append(", Test connection.");
                testsToRun = TestsToRun.VALID_CONNECTION;
            } else if (testMethod.equalsIgnoreCase("ic")) {
                message.append(", Test invalid connection.");
                testsToRun = TestsToRun.INVALID_CONNECTION;
            } else if (testMethod.equalsIgnoreCase("ct")) {
                message.append(", Test connection and topic discovery.");
                testsToRun = TestsToRun.VALID_TOPIC;
            } else if (testMethod.equalsIgnoreCase("all")) {
                message.append(", All test cases.");
                testsToRun = TestsToRun.ALL_TESTS;
            } else {
                message.append(", Wrong test method provided, defaulting to all test cases..");
                testsToRun = TestsToRun.ALL_TESTS;
            }
        } else {
            message.append("No arguments provided, defaulting to localhost:9092 and running all test cases");
            testsToRun = TestsToRun.ALL_TESTS;
        }

        System.out.println(message);

        switch (testsToRun) {
            case VALID_CONNECTION:
                testSuccessfulBrokerConnection();
                break;
            case VALID_TOPIC:
                testSuccessfulBrokerConnection();
                break;
            case ALL_TESTS:
                testSuccessfulBrokerConnection();
                testFailedBrokerConnection();
//                testKeepConnectionOpenAndConsumeMessage();
                testKeepConnectionOpenAndConsumeDummyMessagesMessage();
                break;
        }
    }

    private static void testFailedBrokerConnection() {
        System.out.println("Test invalid broker connection, wait for 60 seconds to timeout...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(WRONG_BOOTSTRAP_SERVER);
        try {
            consumer.listTopics();
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to the broker.");
            System.out.println("TEST SUCCESSFUL");
        }
    }

    private static void testSuccessfulBrokerConnection() {
        System.out.println("About to connect to a valid broker...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER);
        try {
            System.out.println("Topic length should be > 0: " + (consumer.listTopics().size() > 0));
            // Validate that our topic is among them
            System.out.println("Can discover our topic: " + consumer.listTopics().keySet().contains(TOPIC));
            System.out.println("TEST SUCCESSFUL");
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to broker.");
        }
    }

    private static void testKeepConnectionOpenAndConsumeMessage() {
        System.out.println("About to open a connection for 10 seconds to a valid broker and consume some messages...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER);

        // Wait 10 seconds
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < 10000) {
            // Consumer poll every one second
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            System.out.println("consumer records count: " + consumerRecords.count());
            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("TEST SUCCESSFUL");
    }

    private static void testKeepConnectionOpenAndConsumeDummyMessagesMessage() throws Exception {
        System.out.println("Test to open a connection for 10 seconds to a valid broker and consume dummy messages...");
        final var dummyMessageCount = 50;
        ProducerUtil.runProducer(dummyMessageCount);
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER);

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
        System.out.println("TEST SUCCESSFUL");
    }

    private enum TestsToRun {
        ALL_TESTS, VALID_CONNECTION, INVALID_CONNECTION, VALID_TOPIC, INVALID_TOPIC
    }
}
