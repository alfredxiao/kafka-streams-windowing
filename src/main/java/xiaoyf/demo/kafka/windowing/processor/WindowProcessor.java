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

@Component
@Slf4j
public class WindowProcessor {

    @Autowired
    void process(StreamsBuilder streamsBuilder) {
        //Serde<Windowed<String>> winSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(10).toMillis());
        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> source = streamsBuilder.stream(
                "source",
                Consumed.with(new UtcFormatTimestampExtractor()));

        Materialized<String, String, WindowStore<Bytes, byte[]>> aggMat =
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("aggregated-store")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde);

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
                                return aggregate + ", " + value;
                            }
                        },
                        Named.as("aggregate"),
                        aggMat
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream();

        aggregated
                .selectKey((k, v) -> k.key(), Named.as("win-to-norm"))
                .peek((key, value) -> log.info("Emitting {} {}", key, value))
                .to("output");
    }
}
