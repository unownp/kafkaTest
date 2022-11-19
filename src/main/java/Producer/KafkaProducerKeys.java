package Producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
class KafkaProducerKeys {

    public static void main(String[] args) {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProducerDemo.bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //create producer record


        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String key = "id" + i;
            String value = "random" + i;

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);


            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("key:{}", key);
                    log.info("received metaData \n" +
                            "Topic:" + metadata.topic() + "\n" +
                            "Partition " + metadata.partition() + "\n" +
                            "Offset " + metadata.offset() + "\n" +
                            "Timeset " + metadata.timestamp());
                } else log.error("error while producing", exception);
            });
        }
        // kafkaProducer.flush();
        kafkaProducer.close();
    }
}
