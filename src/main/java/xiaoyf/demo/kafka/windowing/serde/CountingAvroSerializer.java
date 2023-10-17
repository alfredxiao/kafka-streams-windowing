package xiaoyf.demo.kafka.windowing.serde;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import static xiaoyf.demo.kafka.windowing.serde.ConfigurationUtils.withSpecificAvroEnabled;
import static xiaoyf.demo.kafka.windowing.serde.CountingAvroSerdeStatsLogger.logg;

public class CountingAvroSerializer<T extends org.apache.avro.specific.SpecificRecord>
        implements Serializer<T> {

    private final KafkaAvroSerializer inner;

    public CountingAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    @Override
    public void configure(final Map<String, ?> serializerConfig,
                          final boolean isSerializerForRecordKeys) {
        inner.configure(
                withSpecificAvroEnabled(serializerConfig),
                isSerializerForRecordKeys);
    }

    @Override
    public byte[] serialize(final String topic, final T record) {
        return serialize(topic, null, record);
    }

    @Override
    public byte[] serialize(final String topic, final Headers headers, final T record) {
        increaseAndMaybePrintCount(topic);
        logg("Serializing " + topic + ":" + record);
        return inner.serialize(topic, headers, record);
    }

    @Override
    public void close() {
        inner.close();
    }

    static Map<String, Long> countsByTopic = new HashMap<>();
    private void increaseAndMaybePrintCount(String topic) {
        countsByTopic.putIfAbsent(topic, 0L);
        countsByTopic.computeIfPresent(topic, (k, c) -> c+1);
        logg("Serialize " + topic + " -> " + countsByTopic.get(topic));
    }

}