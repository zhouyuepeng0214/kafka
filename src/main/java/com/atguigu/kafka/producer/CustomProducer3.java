package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducer3 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop110:9092,hadoop111:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.LINGER_MS_CONFIG,1000);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG,1);

        //矿建一个kafkaproducer对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //2.调用send方法
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", i + "", "message" + i);
            RecordMetadata recordMetadata = producer.send(record).get();

            if (recordMetadata == null) {

            } else {
                System.out.println("success:"+recordMetadata.topic()+"-"+recordMetadata.partition()+"-"+recordMetadata.offset());
            }
        }

        producer.close();
    }
}
