package de.predic8.j_serialization;

import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static java.lang.Math.random;
import static java.lang.Math.round;

public class ArtikelProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ACKS_CONFIG, "all");
        props.put(RETRIES_CONFIG, 0);
        props.put(BATCH_SIZE_CONFIG, 16000);
        props.put(LINGER_MS_CONFIG, 100);
        props.put(BUFFER_MEMORY_CONFIG, 33554432);

        try(Producer<Long, Artikel> producer = new KafkaProducer<>(props, new LongSerializer(), new ArtikelSerde())) {

            int i = 0;
            long t1 = System.currentTimeMillis();

            for (; i < 10; i++) {

                Long key = round(random() * 1000);                

                producer.send(new ProducerRecord<>("artikel", key, new Artikel(key, "name", 3.99f)));
            }
            System.out.println("fertig " + i + " Nachrichten in " + (System.currentTimeMillis() - t1 + " ms"));
            producer.close();
        }


    }
}
