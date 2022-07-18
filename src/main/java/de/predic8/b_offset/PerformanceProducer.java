package de.predic8.b_offset;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.json.Json;
import javax.json.JsonObject;
import java.util.Properties;

import static java.lang.Math.random;
import static java.lang.Math.round;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class PerformanceProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(BATCH_SIZE_CONFIG, 2_000);
        props.put(LINGER_MS_CONFIG, 1);
        //props.put(COMPRESSION_TYPE_CONFIG, "gzip");

        /**
         * 100 bytes
         * 1ms
         * -> erst nach 100ms wird geschrieben
         * 
         * Zeit: 1.493 Sek.
         * Zeit: 1.493 Sek.
         * Zeit: 1.749 Sek.
         * 
         * 
         * Zeit: 10.475 Sek.
         * Zeit: 10.806 Sek.
         * Zeit: 10.855 Sek.
         */

        try(Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {


            long t1 = System.currentTimeMillis();

            for (int i = 1; i <= 1_000_000; i++) {

                JsonObject json = Json.createObjectBuilder()
                .add("windrad", 6)
                .add("kw/h", 33)
                .build();

                String msg = json.toString();

                String key = String.valueOf(round(random() * 1000));

                producer.send(new ProducerRecord<>("produktion", key, msg));
            }
            System.out.println("Zeit: " + ((System.currentTimeMillis() - t1) / 1000f) + " Sek.");

        }
    }
}
