import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerUtil {

    private static final String TOPIC = "my-topic";

//    static Consumer<Long, String> createConsumer(String serverName) {
//        final Properties props = new Properties();
//
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverName);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//
//        // Create the consumer using props.
//        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
//
//        // Subscribe to the topic.
//        consumer.subscribe(Collections.singletonList(TOPIC));
//
//        return consumer;
//    }

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
