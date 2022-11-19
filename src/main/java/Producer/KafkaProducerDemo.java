package Producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public
class KafkaProducerDemo {
    public static final String bootStrapServer = "127.0.0.1:9092";

    public static void main(String[] args) {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("first_topic", "something awesome");

        //send data
        IntStream.range(0, 10).mapToObj(i -> producerRecord).forEach(kafkaProducer::send);
        // kafkaProducer.flush();
        kafkaProducer.close();
    }
}
