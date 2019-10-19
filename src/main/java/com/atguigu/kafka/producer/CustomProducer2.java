package com.atguigu.kafka.producer;

import com.atguigu.kafka.interceptor.CountInterceptor;
import com.atguigu.kafka.interceptor.TimeInterceptor;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer2 {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop110:9092,hadoop111:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG,1);

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,TimeInterceptor.class.getName() + "," + CountInterceptor.class.getName());

        //矿建一个kafkaproducer对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //2.调用send方法
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", i + "", "message" + i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        System.out.println("success:" + metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

        }

        producer.close();
    }
}
