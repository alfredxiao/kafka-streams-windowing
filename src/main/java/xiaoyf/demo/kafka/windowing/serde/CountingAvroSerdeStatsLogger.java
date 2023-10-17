package xiaoyf.demo.kafka.windowing.serde;

import lombok.SneakyThrows;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class CountingAvroSerdeStatsLogger {
    static boolean ENABLED = false;

    @SneakyThrows
    public static void logg(Object msg) {
        if (ENABLED) {
            FileWriter fw = new FileWriter("stats.log", true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(msg == null ? "" : msg.toString());
            bw.newLine();
            bw.close();
        }
    }
}
