package xiaoyf.demo.kafka.windowing.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

import static xiaoyf.demo.kafka.windowing.serde.ConfigurationUtils.withSpecificAvroEnabled;
import static xiaoyf.demo.kafka.windowing.serde.CountingAvroSerdeStatsLogger.logg;

public class CountingAvroDeserializer<T extends org.apache.avro.specific.SpecificRecord>
        implements Deserializer<T> {

    private final KafkaAvroDeserializer inner;

    public CountingAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    @Override
    public void configure(final Map<String, ?> deserializerConfig,
                          final boolean isDeserializerForRecordKeys) {
        inner.configure(
                withSpecificAvroEnabled(deserializerConfig),
                isDeserializerForRecordKeys);
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        return deserialize(topic, null, bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(final String topic, final Headers headers, final byte[] bytes) {
        increaseAndMaybePrintCount(topic);
        T result = (T) inner.deserialize(topic, headers, bytes);

        logg("Deserialized " + topic + " : " + result.toString());
        return result;
    }

    @Override
    public void close() {
        inner.close();
    }

    static Map<String, Long> countsByTopic = new HashMap<>();
    private void increaseAndMaybePrintCount(String topic) {
        countsByTopic.putIfAbsent(topic, 0L);
        countsByTopic.computeIfPresent(topic, (k, c) -> c+1);
        logg("Deserialize " + topic + " -> " + countsByTopic.get(topic));
    }
}