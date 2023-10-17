package xiaoyf.demo.kafka.windowing.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.HashMap;
import java.util.Map;

public class ConfigurationUtils {

    static Map<String, Object> withSpecificAvroEnabled(final Map<String, ?> config) {
        Map<String, Object> specificAvroEnabledConfig =
                config == null ? new HashMap<String, Object>() : new HashMap<>(config);
        specificAvroEnabledConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return specificAvroEnabledConfig;
    }

}
