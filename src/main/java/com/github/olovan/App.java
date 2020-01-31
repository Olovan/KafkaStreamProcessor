package com.github.olovan;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

public class App 
{

    private static String APP_ID = "wordcount-test";
    private static String BROKER_IP = "localhost:9092";
    private static String STATE_STORE = "/tmp/kafka-state-store";

    public static void main( String[] args )
    {
        Properties props = createKafkaProperties();
        StreamsBuilder builder = new StreamsBuilder();
        StreamProcessor.configureSerdes(Collections.singletonMap("schema.registry.url", "http://localhost:8081"));
        StreamProcessor.createStream(builder);
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties createKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, APP_ID + "-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_IP);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000); // Commit every 10 seconds
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_STORE);
        return props;
    }
}
