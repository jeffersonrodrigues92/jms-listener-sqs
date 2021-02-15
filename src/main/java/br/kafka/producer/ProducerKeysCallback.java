package br.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeysCallback {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerKeysCallback.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        final KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

        // create producer record

        for( int i = 0; i<10; i++) {

            String topic = "first_topic";
            String value = "hello world";
            String key = "id_" + i;

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key: " + key);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    // execute every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        logger.info("Received new metadata:\n"
                                        .concat("Topic: ".concat(recordMetadata.topic()).concat("\n")
                                            .concat("Partition: ".concat(String.valueOf(recordMetadata.partition())).concat("\n")
                                                .concat("Offset: ".concat(String.valueOf(recordMetadata.offset())).concat("\n")
                                                        .concat("Timestamp: ").concat(String.valueOf(recordMetadata.timestamp()))))));
                    } else
                        logger.error("Error while producing", exception);
                }
            }).get(); // block the .send() to make it synchronous - don't do this in production
        }
        // flush data
        producer.flush();

        // flush close
        producer.close();;

    }
}
