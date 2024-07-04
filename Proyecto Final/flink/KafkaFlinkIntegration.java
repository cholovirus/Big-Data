import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.ArrayList;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class KafkaFlinkIntegration {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "localhost:9092";
        String groupId = "my-group";
        String topic = "quickstart-events";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        
        String topic2 = "events";
        Properties properties2 = new Properties();
        properties2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties2);

        Random random = new Random();

        consumer.subscribe(Collections.singletonList(topic));
        List<Person> people = new ArrayList<>();
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String, String> record : records) {
                // System.out.printf("consumir llave: %s, valor: %s%n", record.key(), record.value());
                String[] parts = record.value().split(","); // Suponiendo que value está en formato "name, age"
                String name = parts[0].trim();
                String title = parts[1].trim();
                
                Person person = new Person(name, title);
                
                // Agregar el objeto Person a la lista
                people.add(person);
            }
            
            if (people.isEmpty()) {
                //System.out.print("Hola, ");
                continue;
            }
            // Aquí puedes agregar el flujo de Flink si necesitas procesar los datos de Kafka
            // Ejemplo de cómo podrías integrar Flink con el código de Kafka:
            DataStream<Person> flintstones = env.fromCollection(people);

            DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
                @Override
                public boolean filter(Person person) throws Exception {
                    return person.name != "Cewbot";
                }
            });
            adults.print();

            // Convertir el DataStream<Person> a un DataStream<String> para Kafka
            DataStream<String> resultStream = adults.map(new MapFunction<Person, String>() {
            @Override
            public String map(Person person) throws Exception {
                
                String personJson = String.format("{\"name\": \"%s\", \"age\": \"%s\"}", person.getName(), person.getAge());
                return personJson;
            }
            });
            resultStream.print();
            
            ProducerRecord<String,String> record = new ProducerRecord<>(topic2,"enviado", String.valueOf(people.size()));
            
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Mandar llave: " + "llave" + ", valor: " + "mi-valor");
                } else {
                    exception.printStackTrace();
                }
            });
            int randomNumber = random.nextInt(11);
            if (randomNumber ==3 ||  randomNumber ==6){
                people = new ArrayList<>();
            }            
            //ProducerRecord<String, String> record = new ProducerRecord<>(topic2, , String.valueOf(numberOfPeople););
            env.execute("Kafka Flink Integration");
        }
        
    }

    // Clase de ejemplo para Person
    static class Person {
        String name;
        String titulo;

        public Person(String name, String titulo) {
            this.name = name;
            this.titulo = titulo;
        }
        public String getName() {
            return this.name;
        }

        public String getAge() {
            return this.titulo;
        }
    }
}
