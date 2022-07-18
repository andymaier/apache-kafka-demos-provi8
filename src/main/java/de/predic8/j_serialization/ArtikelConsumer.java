package de.predic8.j_serialization;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static java.util.Collections.singletonList;
import static java.time.Duration.ofSeconds;

public class ArtikelConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, "a");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(SESSION_TIMEOUT_MS_CONFIG, "30000");

        try(KafkaConsumer<Long, Artikel> consumer = new KafkaConsumer<>(props, new LongDeserializer(), new ArtikelSerde()) ) {
            consumer.subscribe( singletonList("artikel"));

            while (true){
                for (ConsumerRecord<Long, Artikel> rec : consumer.poll(ofSeconds(1)))
                    System.out.printf("offset= %d, key= %s, value= %s\n", rec.offset(), rec.key(), rec.value());
            }
        }
    }
}
