import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class productor {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "quickstart-events";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "marcelo", "sebastian");

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Mandar llave: " + "llave" + ", valor: " + "mi-valor");
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }
}
