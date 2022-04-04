import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;

public class ConsumerTest {

    private static final String TOPIC = "my-topic-1";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String WRONG_BOOTSTRAP_SERVER = "localhost:9066";

    public static void main(String[] args) throws Exception {
//        testFailedBrokerConnection();
//        testSuccessfulBrokerConnection();

        testConsumeMessages();
    }

    private static void testFailedBrokerConnection() {
        System.out.println("About to connect to broker...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(WRONG_BOOTSTRAP_SERVER);
        try {
            consumer.listTopics();
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to broker.");
        }
    }

    private static void testSuccessfulBrokerConnection() {
        System.out.println("About to connect to a valid broker...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER);
        try {
            System.out.println("Topic length is greater than zero: " + consumer.listTopics().size());
            // Validate that our topic is among them
            System.out.println("We can see our topic: " + consumer.listTopics().keySet().contains(TOPIC));
        } catch (TimeoutException e) {
            System.out.println("Failed to connect to broker.");
        }
    }

    private static void testConsumeMessages() {
        System.out.println("About to open a connection for 1 minute to a valid broker and consume some messages...");
        final Consumer<Long, String> consumer = ConsumerUtil.createConsumer(BOOTSTRAP_SERVER);

        // Wait 10 seconds
        long startTime = System.currentTimeMillis(); //fetch starting time
        while (false || (System.currentTimeMillis() - startTime) < 10000) {
            // Consumer poll every one second
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            System.out.println("consumerRecords count: " + consumerRecords.count());
            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("DONE");
    }
}
