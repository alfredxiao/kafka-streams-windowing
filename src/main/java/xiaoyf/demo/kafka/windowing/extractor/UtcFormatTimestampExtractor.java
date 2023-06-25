package xiaoyf.demo.kafka.windowing.extractor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.format.annotation.DateTimeFormat;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// Expects datetime string like "2000-01-01T02:40:01Z"
public class UtcFormatTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() == null) {
            return -1;
        }

        if (!(record.value() instanceof String)) {
            return -1;
        }

        final String value = (String) record.value();

        if (value.isBlank()) {
            return -1;
        }

        try {
            return parse(value);
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private long parse(final String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

}
