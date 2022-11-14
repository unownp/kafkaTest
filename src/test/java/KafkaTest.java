import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
class KafkaTest {
    public static final String bootStrapServer = "127.0.0.1:9092";

    @Test
    void doSomethingWithKafka() {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("first_topic", "something awesome");

        //send data
        kafkaProducer.send(producerRecord);

        // kafkaProducer.flush();
        kafkaProducer.close();
    }
}
