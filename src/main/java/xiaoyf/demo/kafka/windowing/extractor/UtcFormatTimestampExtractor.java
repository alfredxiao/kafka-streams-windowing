package xiaoyf.demo.kafka.windowing.extractor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.format.annotation.DateTimeFormat;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// yyyy-MM-dd'T'HH:mm:ss'Z', e.g. 2000-01-01T00:00:01.00Z
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

    public static void main(String[] args) {
//        UtcFormtTimestampExtractor e = new UtcFormtTimestampExtractor();
//        long l = e.parse("2000-01-00T01:00:00.000000Z");
//        long l = e.parse("2023-06-03T05:37:45.371962Z");
//        System.out.println(l);

        System.out.println(DateTimeFormatter.ISO_INSTANT);

        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX").withZone(ZoneId.of("UTC"));
//        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        System.out.println(f.format(Instant.now()));
        System.out.println();

        System.out.println(Instant.parse("2018-11-30T18:35:24.000Z").toEpochMilli());
        System.out.println(Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
        System.out.println(Instant.parse("2018-11-30T18:35:24.0Z").toEpochMilli());
        System.out.println(Instant.parse("2018-11-30T18:35:24Z").toEpochMilli());
        System.out.println(Instant.parse("2000-01-01T02:40:00Z").toEpochMilli());
        System.out.println(Instant.now().toString());
        System.out.println(Instant.now().toEpochMilli());

    }
}
