package xiaoyf.demo.kafka.windowing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import xiaoyf.demo.kafka.windowing.config.ApplicationProperties;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(ApplicationProperties.class)
public class KafkaWindowingApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaWindowingApplication.class, args);
	}

}

