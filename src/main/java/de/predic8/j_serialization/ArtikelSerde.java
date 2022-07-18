package de.predic8.j_serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class ArtikelSerde implements Serializer<Artikel>, Deserializer<Artikel> {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        System.out.println("map = " + map);
        System.out.println(b);
    }

    @Override
    public Artikel deserialize(String s, byte[] bytes) {
        try {
            return mapper.readValue(bytes, Artikel.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public byte[] serialize(String s, Artikel artikel) {
        try {
            return mapper.writeValueAsBytes(artikel);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {
        mapper = null;
    }
}
