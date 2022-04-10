package team.antipython.kafkatest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class Publisher {


    /*
    這是一個隨機發送到 Topic-1 的任意 partition 的 Producer
     */
    public static void main(String[] args){

        Scanner scanner = new Scanner(System.in);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        while (true){
            String input = scanner.nextLine();
            int partition = random.nextInt(3)-1;
            System.out.println("Sending "+input+" to Topic-1 p:"+partition);
            producer.send(new ProducerRecord<>("topic-1",partition,"Any Key",input));
        }
    }

}
