package com.github.olovan;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamProcessorTest 
{

    TopologyTestDriver driver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setup() {
        Properties props = App.createKafkaProperties(); // Reuse properties from app
        StreamsBuilder builder = new StreamsBuilder();
        StreamProcessor.createStream(builder);
        driver = new TopologyTestDriver(builder.build(), props);
        inputTopic = driver.createInputTopic("Messages", new StringSerializer(), new StringSerializer());
        outputTopic = driver.createOutputTopic("Words", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void tearDown() {
        driver.close();
    }

    @Test
    public void shouldCountWords() {
        inputTopic.pipeInput("HELLO WORLD HELLO");
        Map<String, Long> results = outputTopic.readKeyValuesToMap();
        assertTrue(results.get("HELLO") == 2L);
        assertTrue(results.get("WORLD") == 1L);
    }

    @Test
    public void shouldSumWordsAcrossMultipleMessages() {
        inputTopic.pipeInput("HELLO WORLD HELLO");
        inputTopic.pipeInput("TEST TEST TEST");
        inputTopic.pipeInput("TEST TEST TEST");
        Map<String, Long> results = outputTopic.readKeyValuesToMap();
        assertTrue(results.get("HELLO") == 2L);
        assertTrue(results.get("WORLD") == 1L);
        assertTrue(results.get("TEST") == 6L);
    }
}
