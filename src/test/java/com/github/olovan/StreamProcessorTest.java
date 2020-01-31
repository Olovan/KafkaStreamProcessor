package com.github.olovan;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import DataContract.Item.FullItem;
import DataContract.Item.FullItemKey;
import DataContract.Item.Item;
import DataContract.Item.ItemDanger;
import DataContract.Item.ItemDangerKey;
import DataContract.Item.ItemKey;
import DataContract.Item.ItemType;
import DataContract.Item.ItemTypeKey;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class StreamProcessorTest 
{

    TopologyTestDriver driver;
    TestInputTopic<ItemKey, Item> itemInput;
    TestInputTopic<ItemTypeKey, ItemType> typeInput;
    TestInputTopic<ItemDangerKey, ItemDanger> dangerInput;
    TestOutputTopic<FullItemKey, FullItem> outputTopic;

    @Before
    public void setup() {
        Properties props = App.createKafkaProperties(); // Reuse properties from app
        StreamsBuilder builder = new StreamsBuilder();
        overrideDefaultSerdes();
        StreamProcessor.createStream(builder);
        driver = new TopologyTestDriver(builder.build(), props);
        itemInput = driver.createInputTopic("Item", StreamProcessor.itemSerdes.key.serializer(), StreamProcessor.itemSerdes.value.serializer());
        typeInput = driver.createInputTopic("ItemType", StreamProcessor.itemTypeSerdes.key.serializer(), StreamProcessor.itemTypeSerdes.value.serializer());
        dangerInput = driver.createInputTopic("ItemDanger", StreamProcessor.itemDangerSerdes.key.serializer(), StreamProcessor.itemDangerSerdes.value.serializer());
        outputTopic = driver.createOutputTopic("FullItem", StreamProcessor.fullItemSerdes.key.deserializer(), StreamProcessor.fullItemSerdes.value.deserializer());
    }

    void overrideDefaultSerdes() {
        Map<String, Object> serdesConfig = Collections.singletonMap("schema.registry.url", "FAKE");
        MockSchemaRegistryClient fakeClient = new MockSchemaRegistryClient();
        StreamProcessor.itemSerdes = new KeyValue<>(
            new SpecificAvroSerde<>(fakeClient),
            new SpecificAvroSerde<>(fakeClient)
        );
        StreamProcessor.itemTypeSerdes = new KeyValue<>(
            new SpecificAvroSerde<>(fakeClient),
            new SpecificAvroSerde<>(fakeClient)
        );
        StreamProcessor.itemDangerSerdes = new KeyValue<>(
            new SpecificAvroSerde<>(fakeClient),
            new SpecificAvroSerde<>(fakeClient)
        );
        StreamProcessor.fullItemSerdes = new KeyValue<>(
            new SpecificAvroSerde<>(fakeClient),
            new SpecificAvroSerde<>(fakeClient)
        );
        StreamProcessor.configureSerdes(serdesConfig);
    }

    @After
    public void tearDown() {
        try {
            driver.close();
        } catch(Exception e) {
            //Don't care
        }
    }

    @Test
    public void givenItemShouldGenerateOutput() {
        itemInput.pipeInput(new ItemKey(1), new Item(1, "Name", 0, 0));
        var result = outputTopic.readKeyValue();
        Assert.assertEquals("Name", result.value.getName().toString());
    }

    @Test
    public void givenTypeShouldIncludeTypeInformation() {
        typeInput.pipeInput(new ItemTypeKey(2), new ItemType(2, "Type"));
        itemInput.pipeInput(new ItemKey(1), new Item(1, "Name", 2, 0));
        var result = outputTopic.readKeyValue();
        Assert.assertEquals("Type", result.value.getTypeDescription().toString());
    }

    @Test
    public void givenDangerShouldIncludeDangerInformation() {
        dangerInput.pipeInput(new ItemDangerKey(3), new ItemDanger(3, "DANGER"));
        itemInput.pipeInput(new ItemKey(1), new Item(1, "Name", 2, 3));
        var result = outputTopic.readKeyValue();
        Assert.assertEquals("DANGER", result.value.getDangerDescription().toString());
    }
}
