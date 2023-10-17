package xiaoyf.demo.kafka.windowing.support;

import demo.model.Measurement;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class LoadTestProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
		    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Measurement> producer = new KafkaProducer<>(props);

        String key = "solar_a";
        Measurement value = Measurement.newBuilder()
                .setSubject(key)
                .setMeasurement(200L)
                .setMeasurementId(0L)
                .setWhen("2000-01-01T01:00:00Z")
                .build();

        ProducerRecord<String, Measurement> record = new ProducerRecord<>(
                "measurement", 0, Instant.now().toEpochMilli(),
                key, value);

        try {
            int max = 5;
            int messagesPerSecond = 30;

            boolean largeVolumeMode = max > messagesPerSecond; // when max is high, set to true

            int estimatedTimeSpent = max/messagesPerSecond; // 9000/30 -> 300s
            int sent = 0;

            long start = System.currentTimeMillis();
            System.out.printf("##!! mps:%d, max:%d, estimate timespent:%d\n", messagesPerSecond, max, estimatedTimeSpent);
            for (int i=0; i<max; i++) {
                producer.send(record);
                sent++;

                if (sent % 500 == 0){
                    System.out.printf("##!! sent:%d, timespent: %d\n", sent, System.currentTimeMillis() - start);
                }

                if (largeVolumeMode == true) {
                    Thread.sleep(1000 / messagesPerSecond);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
