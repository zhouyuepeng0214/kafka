package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidCommitOffsetSizeException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class CustomOffset {

    private static Map<TopicPartition,Long> currentOffsets = new HashMap<TopicPartition, Long>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop110:9092,hadoop111:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigData-0311");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("first"),new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffsets.clear();
                for (TopicPartition partition : partitions) {
                    Long offset = getOffsetByTopicPartition(partition);
                    consumer.seek(partition,offset);
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    currentOffsets.put(new TopicPartition(record.topic(),record.partition()),record.offset());
                }
                commitOffset(currentOffsets);
            }

        } finally {
            consumer.close();

        }
    }

    private static void commitOffset(Map<TopicPartition,Long> currentOffsets) {
    }

    private static Long getOffsetByTopicPartition(TopicPartition partition) {
        return null;
    }
}
