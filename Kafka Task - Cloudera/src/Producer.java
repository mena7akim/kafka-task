import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import scala.collection.JavaConversions;

// Import classes for topic management in Kafka 0.9.0.0
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class Producer {

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

    public static void main(String[] argv) {
        int port = 9092;
        String topicName = "task1";
        String zookeeperConnect = "localhost:2181";
        
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
        
        String bootstrapServers = "localhost:" + port;
        System.out.println("Producer configured with:");
        System.out.println("- Bootstrap Server: " + bootstrapServers);
        System.out.println("- Topic Name: " + topicName);
        
        // Check and create topic if it doesn't exist
        createTopicIfNotExists(zookeeperConnect, topicName);
        
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configProperties);
        Scanner in = new Scanner(System.in);

        System.out.println("Enter message (type 'exit' to quit):");
        String message = in.nextLine();
        
        while (!message.equals("exit")) {
            try {
                System.out.println("Sending: " + message);
                producer.send(new ProducerRecord<String, String>(topicName, message));
                System.out.println("Message sent successfully");
            } catch (Exception e) {
                System.err.println("Error sending message: " + e.getMessage());
            }
            
            System.out.println("Enter message (type 'exit' to quit):");
            message = in.nextLine();
        }
        
        in.close();
        producer.close();
        System.out.println("Producer closed");
    }
}