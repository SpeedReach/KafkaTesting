package team.antipython.kafkatest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class Subscriber {

    /*
     * 這是一個只處理 Topic-1 的 partition 1 的 Consumer
     */
    public static void main(String[] args){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final Consumer<String,String> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Collections.singleton(TopicIDs.Topic1));
        consumer.subscribe(Arrays.asList(TopicIDs.TOPICS));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record->{
                System.out.println("Topic: "+record.topic() +" ,Key: "+ record.key() + " ,Value: "+record.value());
            });
        }

    }
}












