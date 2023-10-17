package xiaoyf.demo.kafka.windowing.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix="kafka-windowing")
@Data
public class ApplicationProperties {
    String measurementTopic;
    String aggregatedMeasurementsTopic;
    Duration windowSize;
    Duration grace;
}
