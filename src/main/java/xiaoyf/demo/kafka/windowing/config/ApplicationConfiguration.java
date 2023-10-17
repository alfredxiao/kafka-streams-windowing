package xiaoyf.demo.kafka.windowing.config;

import demo.model.MeasurementAggregated;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xiaoyf.demo.kafka.windowing.serde.CountingAvroSerde;

@Configuration
public class ApplicationConfiguration {

    @Bean
    Serde<MeasurementAggregated> measurementAggregatedSerde(KafkaProperties kafkaProperties) {
        Serde<MeasurementAggregated> serde = new CountingAvroSerde<>();
        serde.configure(kafkaProperties.buildStreamsProperties(), false);

        return serde;
    }
}
