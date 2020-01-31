package com.github.olovan;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import DataContract.Item.FullItem;
import DataContract.Item.FullItemKey;
import DataContract.Item.Item;
import DataContract.Item.ItemDanger;
import DataContract.Item.ItemDangerKey;
import DataContract.Item.ItemKey;
import DataContract.Item.ItemType;
import DataContract.Item.ItemTypeKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class StreamProcessor {
    static KeyValue<Serde<ItemKey>, Serde<Item>> itemSerdes = new KeyValue<>(new SpecificAvroSerde<>(),
            new SpecificAvroSerde<>());

    static KeyValue<Serde<ItemTypeKey>, Serde<ItemType>> itemTypeSerdes = new KeyValue<>(new SpecificAvroSerde<>(),
            new SpecificAvroSerde<>());

    static KeyValue<Serde<ItemDangerKey>, Serde<ItemDanger>> itemDangerSerdes = new KeyValue<>(
            new SpecificAvroSerde<>(), new SpecificAvroSerde<>());

    static KeyValue<Serde<FullItemKey>, Serde<FullItem>> fullItemSerdes = new KeyValue<>(new SpecificAvroSerde<>(),
            new SpecificAvroSerde<>());

    public static void createStream(StreamsBuilder builder) {

        KStream<ItemKey, Item> itemStream = builder.stream("Item", Consumed.with(itemSerdes.key, itemSerdes.value));

        KTable<Integer, ItemType> itemTypeTable = builder
                .stream("ItemType", Consumed.with(itemTypeSerdes.key, itemTypeSerdes.value))
                .selectKey((k, v) -> k.getID())
                .groupByKey(Grouped.with(Serdes.Integer(), itemTypeSerdes.value))
                .reduce((past, current) -> current,
                    Materialized.with(Serdes.Integer(), itemTypeSerdes.value));

        KTable<Integer, ItemDanger> itemDangerTable = builder
                .stream("ItemDanger", Consumed.with(itemDangerSerdes.key, itemDangerSerdes.value))
                .selectKey((k, v) -> k.getID())
                .groupByKey(Grouped.with(Serdes.Integer(), itemDangerSerdes.value))
                .reduce((past, current) -> current,
                    Materialized.with(Serdes.Integer(), itemDangerSerdes.value));

        itemStream
            .selectKey((k, v) -> v.getID())
            .to("INTERNAL_1", Produced.with(Serdes.Integer(), itemSerdes.value));
        builder.table("INTERNAL_1", Consumed.with(Serdes.Integer(), itemSerdes.value))
            .leftJoin(itemTypeTable, (i) -> i.getTypeID(), 
                (i, it) -> it == null ?
                    new FullItem(i.getID(), i.getName(), i.getTypeID(), "", i.getDangerID(), "") :
                    new FullItem(i.getID(), i.getName(), i.getTypeID(), it.getDescription(), i.getDangerID(), ""),
                Materialized.with(Serdes.Integer(), fullItemSerdes.value))
            .toStream()
            .to("INTERNAL_2", Produced.with(Serdes.Integer(), fullItemSerdes.value));
            
        builder.table("INTERNAL_2", Consumed.with(Serdes.Integer(), fullItemSerdes.value))
            .leftJoin(itemDangerTable, (fi) -> fi.getDangerID(), 
                (fi, id) -> id == null ?
                    fi : 
                    FullItem.newBuilder(fi).setDangerDescription(id.getDescription()).build(), 
                Materialized.with(Serdes.Integer(), fullItemSerdes.value))
            .toStream()
            .selectKey((k, v) -> new FullItemKey(v.getID()))
            .to("FullItem", Produced.with(fullItemSerdes.key, fullItemSerdes.value));
    }

    public static void configureSerdes(Map<String, Object> serdesConfig) {
        itemSerdes.key.configure(serdesConfig, true);
        itemSerdes.value.configure(serdesConfig, false);
        itemTypeSerdes.key.configure(serdesConfig, true);
        itemTypeSerdes.value.configure(serdesConfig, false);
        itemDangerSerdes.key.configure(serdesConfig, true);
        itemDangerSerdes.value.configure(serdesConfig, false);
        fullItemSerdes.key.configure(serdesConfig, true);
        fullItemSerdes.value.configure(serdesConfig, false);
    }
}