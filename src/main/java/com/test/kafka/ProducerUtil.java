package com.test.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerUtil {

    static void runProducer(String bootStrapServer, final int sendMessageCount, String topic) throws ExecutionException, InterruptedException {
        final Producer<Long, String> producer = createProducer(bootStrapServer);
        long time = System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                var producedTime = System.currentTimeMillis();
                final ProducerRecord<Long, String> record = new ProducerRecord<>(topic, index, "A message, Milli time: " + producedTime + index);

                producer.send(record).get();
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static Producer<Long, String> createProducer(String bootStrapServer) {
        var props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }
}
