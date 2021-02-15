package br.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerGroupsThreads {

    private Logger logger = LoggerFactory.getLogger(ConsumerGroupsThreads.class);

    public static void main(String[] args) {
            new ConsumerGroupsThreads().run();

    }

    private void run(){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-seventh-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info( "Creating the consumer thread");
        Runnable consumerThreads = new ConsumerThread(bootstrapServers, groupId, topic, latch);

        Thread thread = new Thread(consumerThreads);
        thread.start();


        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) consumerThreads).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                logger.info("Application has exited");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String bootstrapServers;
        private String topic;
        private String groupId;

        private Logger logger = LoggerFactory.getLogger(ConsumerGroupsThreads.class);

        public ConsumerThread(String bootstrapServers, String topic, String groupId, CountDownLatch latch){
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.groupId = groupId;
            this.latch = latch;
        }

        @Override
        public void run() {

            Properties properties = new Properties();

            // create consumer configs
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumers
            consumer = new KafkaConsumer<>(properties);

            // poll for new data
            consumer.subscribe(Arrays.asList(topic));

            try {

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " , Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            }catch (WakeupException exception){
                logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            //the wakeup() is an especial method to interrup consumer.
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
