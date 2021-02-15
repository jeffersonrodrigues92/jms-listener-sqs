package br.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        final KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

        // create producer record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(System.getenv("TOPIC_NAME"), null);

        //send data - asynchronous
        producer.send(record, (recordMetadata, exception) -> {
            // execute every time a record is successfully sent or an exception is thrown
            if(exception == null){
                logger.info("Received new metadata:\n"
                        .concat("Topic: ".concat(recordMetadata.topic()).concat("\n")
                        .concat("Partition: ".concat(String.valueOf(recordMetadata.partition())).concat("\n")
                        .concat("Offset: ".concat(String.valueOf(recordMetadata.offset())).concat("\n")
                        .concat("Timestamp: ").concat(String.valueOf(recordMetadata.timestamp()))
                        ))));
            }else
                logger.error("Error while producing", exception);
        });

        // flush data
        producer.flush();
        // flush close
        producer.close();;

    }
}
