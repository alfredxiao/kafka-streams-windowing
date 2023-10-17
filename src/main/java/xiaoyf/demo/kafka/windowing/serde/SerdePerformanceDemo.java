package xiaoyf.demo.kafka.windowing.serde;

import demo.model.Measurement;
import demo.model.MeasurementAggregated;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static xiaoyf.demo.kafka.windowing.serde.ConfigurationUtils.withSpecificAvroEnabled;

public class SerdePerformanceDemo {
    public static void main(String[] args) {

        for (int i=0; i<100; i++) {
            performanceRun(3000, 100);
        }


        performanceRun(9000, 30);
    }

    static void performanceRun(int len, int repeat) {
        MeasurementAggregated aggregated = genMeasurementAggregated(len);

        Serde<MeasurementAggregated> serde = getSerde();
        Serializer<MeasurementAggregated> serializer = serde.serializer();

        Deserializer<MeasurementAggregated> deserializer = serde.deserializer();
        byte[] longBytes = serializer.serialize("agg", aggregated);

        Serde<Measurement> singleSerde = getSerdeSingle();
        Deserializer<Measurement> singleDeserializer = singleSerde.deserializer();
        Measurement measurement = genMeasurement();
        byte[] singleBytes = singleSerde.serializer().serialize("single", measurement);

        long start = System.currentTimeMillis();
        for (int i=0; i<repeat; i++) {
            Measurement m = singleDeserializer.deserialize("single", singleBytes);
            MeasurementAggregated agg = deserializer.deserialize("agg", longBytes);
            agg.getMeasurements().add(m);
            byte[] bytes = serializer.serialize("agg", aggregated);
        }
        long end = System.currentTimeMillis();

        System.out.println("Time spent for " + len + " is " + (end - start) + " ms");
    }

    private static MeasurementAggregated genMeasurementAggregated(int len) {
        return MeasurementAggregated.newBuilder()
                .setSubject("test")
                .setWinStart("2020-01-01T00:00:00Z")
                .setWinEnd("2020-01-01T00:05:00Z")
                .setGrace(0L)
                .setMeasurements(genMeasurements(len))
                .build();
    }

    private static List<Measurement> genMeasurements(int len) {
        List<Measurement> measurements = new ArrayList<>();
        for (int i=0; i<len; i++) {
            measurements.add(genMeasurement());
        }

        return measurements;
    }

    private static Measurement genMeasurement() {
        return Measurement.newBuilder()
                .setMeasurementId(123L)
                .setMeasurement(897L)
                .setSubject("abc")
                .setWhen(Instant.now().toString())
                .build();
    }

    static Serde<Measurement> getSerdeSingle() {
        Serde<demo.model.Measurement> serde = new SpecificAvroSerde<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", "http://localhost:8081");
        serde.configure(withSpecificAvroEnabled(config), false);

        return serde;
    }

    static Serde<MeasurementAggregated> getSerde() {
        Serde<demo.model.MeasurementAggregated> serde = new SpecificAvroSerde<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", "http://localhost:8081");
        serde.configure(withSpecificAvroEnabled(config), false);

        return serde;
    }
}
