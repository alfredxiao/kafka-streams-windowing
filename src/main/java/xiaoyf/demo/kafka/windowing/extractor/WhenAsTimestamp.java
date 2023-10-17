package xiaoyf.demo.kafka.windowing.extractor;

import demo.model.Measurement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

// Expects datetime string like "2000-01-01T02:40:01Z"
public class WhenAsTimestamp implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() == null) {
            return -1;
        }

        if (!(record.value() instanceof Measurement measurement)) {
            return -1;
        }

        return Instant.parse(measurement.getWhen()).toEpochMilli();
    }

}
