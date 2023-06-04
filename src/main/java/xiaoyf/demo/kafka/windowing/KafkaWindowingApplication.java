package xiaoyf.demo.kafka.windowing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaWindowingApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaWindowingApplication.class, args);
	}

}

