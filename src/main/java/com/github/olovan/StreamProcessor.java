package com.github.olovan;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class StreamProcessor {
    public static void createStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder.stream("Messages");
        KTable<String, Long> wordStream = messageStream
            .flatMapValues((v) -> Arrays.asList(v.split(" ")))
            .groupBy((k, v) -> v)
            .count();
        wordStream.toStream().to("Words", Produced.with(Serdes.String(), Serdes.Long()));
    }
}