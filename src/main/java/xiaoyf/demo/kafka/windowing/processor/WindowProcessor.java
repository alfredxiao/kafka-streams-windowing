package xiaoyf.demo.kafka.windowing.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.windowing.extractor.UtcFormatTimestampExtractor;

import java.time.Duration;

import static xiaoyf.demo.kafka.windowing.support.Consts.INPUT_TOPIC;
import static xiaoyf.demo.kafka.windowing.support.Consts.OUTPUT_TOPIC;
import static xiaoyf.demo.kafka.windowing.support.Consts.WINDOWED_OUTPUT_TOPIC;

@Component
@Slf4j
public class WindowProcessor {

    @Autowired
    void process(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> source = streamsBuilder.stream(
                INPUT_TOPIC,
                Consumed.with(new UtcFormatTimestampExtractor()));

        Materialized<String, String, WindowStore<Bytes, byte[]>> aggregationMaterialized =
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("aggregated-store")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde)
                        .withRetention(Duration.ofDays(1));

        KStream<Windowed<String>, String> aggregated = source
                .groupByKey()
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(1)))
                .aggregate(
                        () -> "",
                        (key, value, aggregate) -> {
                            log.info("Aggregating <- {} | {}", key, value);
                            if (aggregate.isEmpty()) {
                                return value;
                            } else {
                                return aggregate + "," + value;
                            }
                        },
                        Named.as("aggregate"),
                        aggregationMaterialized
                )
                .suppress(Suppressed
                        .untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("suppress-aggregation"))
                .toStream();

        aggregated
                .to(WINDOWED_OUTPUT_TOPIC);

        aggregated
                .selectKey((k, v) -> k.key(), Named.as("win-to-norm"))
                .peek((key, value) -> log.info("Emitting {} {}", key, value))
                .to(OUTPUT_TOPIC);
    }
}
