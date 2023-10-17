package xiaoyf.demo.kafka.windowing.processor;

import demo.model.Measurement;
import demo.model.MeasurementAggregated;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.windowing.config.ApplicationProperties;
import xiaoyf.demo.kafka.windowing.extractor.WhenAsTimestamp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import static xiaoyf.demo.kafka.windowing.support.Consts.INPUT_TOPIC;
import static xiaoyf.demo.kafka.windowing.support.Consts.OUTPUT_TOPIC;

@Component
@RequiredArgsConstructor
@Slf4j
public class WindowProcessor {

    private final ApplicationProperties properties;
    private final Serde<MeasurementAggregated> measurementSerde;

    @Autowired
    void process(StreamsBuilder streamsBuilder) {

        Materialized<String, MeasurementAggregated, WindowStore<Bytes, byte[]>> aggregationMaterialized =
                Materialized.<String, MeasurementAggregated, WindowStore<Bytes, byte[]>>as("aggregated-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(measurementSerde)
                        .withRetention(Duration.ofDays(1));

        streamsBuilder
                .<String, Measurement>stream(properties.getMeasurementTopic(), Consumed.with(new WhenAsTimestamp()))
                .groupByKey()
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                properties.getWindowSize(),
                                properties.getGrace()))
                .<MeasurementAggregated>aggregate(
                        this::emptyAggregation,
                        (key, value, aggregate) -> {
                            if (aggregate.getMeasurements().size() < 10) {
                                aggregate.getMeasurements().add(value);
                            }
                            return aggregate;
                        },
                        Named.as("measurements-aggregated"),
                        aggregationMaterialized
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((wink, value) -> {
                    value.setSubject(wink.key());
                    value.setWinStart(Instant.ofEpochMilli(wink.window().start()).toString());
                    value.setWinEnd(Instant.ofEpochMilli(wink.window().end()).toString());
                    value.setGrace(properties.getGrace().toMillis());
                    return KeyValue.pair(wink.key(), value);
                })
                .to(properties.getAggregatedMeasurementsTopic());
    }

    private MeasurementAggregated emptyAggregation() {
        return MeasurementAggregated.newBuilder()
                .setSubject("")
                .setWinStart("")
                .setWinEnd("")
                .setGrace(0L)
                .setMeasurements(new ArrayList<>())
                .build();
    }
}
