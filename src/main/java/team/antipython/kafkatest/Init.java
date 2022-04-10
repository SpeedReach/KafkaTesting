package team.antipython.kafkatest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Init {



    public static void main(String[] args){

        /*
        Kafka Admin 的 Properties 一般來說直接照抄就好,ip記得改
         */
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Admin admin = Admin.create(props);

        try {


            //Blocking get Topics
            Set<String> existingTopics = admin.listTopics().names().get();


            /*
            Partitions 是該topic的訊息可以分成幾類
            ReplicationFactors 是副本數 必須小於 Broker數量
            由於demo只有一個 broker所以只有一個副本
             */
            int partitions = 3;
            short replicationFactors = 1;
            Map<String,String> topicConfig = Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

            Set<NewTopic> topics =
                    Arrays.stream(TopicIDs.TOPICS)
                            .filter(id->!existingTopics.contains(id))
                            .map(id-> new NewTopic(id,partitions,replicationFactors).configs(topicConfig))
                            .collect(Collectors.toSet());


            CreateTopicsResult result = admin.createTopics(topics);

            /*
             * 這邊才真正執行創建topic,上面都只是在本地寫好參數
             */
            result.all().get();


        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
