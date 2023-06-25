package xiaoyf.demo.kafka.windowing.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static xiaoyf.demo.kafka.windowing.support.Consts.AGGREGATED_STORE_TOPIC;
import static xiaoyf.demo.kafka.windowing.support.Consts.SUPPRESSION_STORE_TOPIC;
import static xiaoyf.demo.kafka.windowing.support.Consts.WINDOWED_OUTPUT_TOPIC;

@Component
@Slf4j
public class WatchProcessor {

    @Autowired
    void process(StreamsBuilder streamsBuilder) {
        Serde<Windowed<String>> winSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(10).toMillis());
        Serde<String> stringSerde = Serdes.String();

        KStream<Windowed<String>, String> windowedStream = streamsBuilder.stream(
                WINDOWED_OUTPUT_TOPIC,
                Consumed.with(winSerde, stringSerde));

        windowedStream
                .peek((k, v) -> {
                    log.info("Seen entry on topic:{}, key:{}, value:{}",
                            WINDOWED_OUTPUT_TOPIC,
                            k,
                            v);
                });

        TimeWindowedSerializer serializer = new TimeWindowedSerializer<>(stringSerde.serializer());
        TimeWindowedDeserializer deserializer = new TimeWindowedDeserializer<>(stringSerde.deserializer(), Duration.ofMinutes(10).toMillis());
        deserializer.setIsChangelogTopic(true);

        Serde serde = Serdes.serdeFrom(
                serializer,
                deserializer
        );

        KStream<Windowed<String>, String> changeLogStream = streamsBuilder.stream(
                AGGREGATED_STORE_TOPIC,
                Consumed.with(serde, stringSerde));

        changeLogStream
                .peek((k, v) -> {
                    log.info("Seen entry on topic:{}, key:{}, value:{}",
                            AGGREGATED_STORE_TOPIC,
                            k,
                            v);
                });

        KStream<Windowed<String>, String> suppressionStream = streamsBuilder.stream(
                SUPPRESSION_STORE_TOPIC,
                Consumed.with(serde, stringSerde));

        suppressionStream
                .peek((k, v) -> {
                    log.info("Seen entry on topic:{}, key:{}, value:{}",
                            SUPPRESSION_STORE_TOPIC,
                            k,
                            v);
                });
    }

}
