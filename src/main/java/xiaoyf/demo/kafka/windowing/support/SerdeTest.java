package xiaoyf.demo.kafka.windowing.support;

import demo.model.MeasurementAggregated;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public class SerdeTest {

    public static void main(String[] args) {
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", "http://localhost:8081");
        SpecificAvroSerde<MeasurementAggregated> serde = new SpecificAvroSerde<MeasurementAggregated>();
        serde.configure(config, false);


    }
}
