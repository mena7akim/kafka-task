import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import scala.collection.JavaConversions;

// Import classes for topic management in Kafka 0.9.0.0
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class Consumer {

    private static void createTopicIfNotExists(String zookeeperConnect, String topicName) {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        
        try {
            // Create a ZooKeeper client
            int sessionTimeoutMs = 15 * 1000; // 15 seconds
            int connectionTimeoutMs = 10 * 1000; // 10 seconds
            zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$
            );
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
            
            // Check if topic exists
            boolean topicExists = AdminUtils.topicExists(zkUtils, topicName);
            
            if (!topicExists) {
                System.out.println("Topic '" + topicName + "' doesn't exist. Creating it...");
                // Create topic with 1 partition and replication factor of 1
                int partitions = 1;
                int replicationFactor = 1;
                Properties topicConfig = new Properties();
                AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor, topicConfig);
                System.out.println("Topic '" + topicName + "' created successfully");
            } else {
                System.out.println("Topic '" + topicName + "' already exists");
            }
        } catch (Exception e) {
            System.err.println("Error while checking/creating topic: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        int port = 9092;
        String topicName = "task1";
        String groupId = "simple-consumer-group";
        String zookeeperConnect = "localhost:2181";
        
        // Parse arguments if provided
        if (argv.length >= 1) {
            try {
                port = Integer.parseInt(argv[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number, using default: 9092");
            }
        }

        if (argv.length >= 2) {
            topicName = argv[1];
        }

        if (argv.length >= 3) {
            groupId = argv[2];
        }

        String bootstrapServers = "localhost:" + port;
        System.out.println("Consumer configured with:");
        System.out.println("- Bootstrap Server: " + bootstrapServers);
        System.out.println("- Topic Name: " + topicName);
        System.out.println("- Group ID: " + groupId);
        
        // Check and create topic if it doesn't exist
        createTopicIfNotExists(zookeeperConnect, topicName);

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message: " + record.value());
                }
            }
        } finally {
            kafkaConsumer.close();
            System.out.println("Consumer closed");
        }
    }
}
