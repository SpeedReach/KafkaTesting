package team.antipython.kafkatest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Init {

    public static void main(String[] args){
        Admin admin = Admin.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
        try {
            Set<String> existingTopics = admin.listTopics().names().get();
            if(!existingTopics.contains("DemoTopic")){
                /*
                Partitions 是該topic的訊息可以分成幾類
                ReplicationFactors 是副本數 必須小於 Broker數量
                由於demo只有一個 broker所以只有一個副本
                */
                int partitions = 3;
                short replicationFactors = 1;
                Set<NewTopic> newTopics = Collections.singleton(new NewTopic("DemoTopic",partitions,replicationFactors));
                admin.createTopics(newTopics).all().get();
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
