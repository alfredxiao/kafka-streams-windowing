package xiaoyf.demo.kafka.windowing.support;

public interface Consts {
    String INPUT_TOPIC = "source";
    String OUTPUT_TOPIC = "output";
    String WINDOWED_OUTPUT_TOPIC = "windowed-output";
    String AGGREGATED_STORE_TOPIC = "kafka-windowing-demo-aggregated-store-changelog";
    String SUPPRESSION_STORE_TOPIC = "kafka-windowing-demo-suppress-aggregation-store-changelog";
}
