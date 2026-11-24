package org.example.analytics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.common.KafkaSettings;
import org.example.common.Topics;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Читает user-events и allowed-products из второго кластера и пишет пачки записей в HDFS:
 * /data/&lt;topic&gt;/batch-&lt;uuid&gt;.json (одна запись на строку). Оффсеты коммитятся после записи пачки.
 */
public class HdfsLoader {

    private static final List<String> TOPICS = List.of(Topics.USER_EVENTS, Topics.ALLOWED_PRODUCTS);

    public static void main(String[] args) throws Exception {
        String hdfsUri = KafkaSettings.env("HDFS_URI", "hdfs://hadoop-namenode:9000");

        Properties props = KafkaSettings.clientProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaSettings.env("GROUP_ID", "analytics-hdfs-loader"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             FileSystem fs = FileSystem.get(new URI(hdfsUri), conf, KafkaSettings.env("HADOOP_USER_NAME", "root"))) {
            consumer.subscribe(TOPICS);
            System.out.println("Загрузчик HDFS запущен: " + TOPICS + " -> " + hdfsUri);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }

                Map<String, StringBuilder> batches = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    batches.computeIfAbsent(record.topic(), t -> new StringBuilder())
                            .append(record.value()).append('\n');
                }

                for (Map.Entry<String, StringBuilder> batch : batches.entrySet()) {
                    Path dir = new Path("/data/" + batch.getKey());
                    fs.mkdirs(dir);
                    Path file = new Path(dir, "batch-" + UUID.randomUUID() + ".json");
                    try (FSDataOutputStream out = fs.create(file)) {
                        out.write(batch.getValue().toString().getBytes(StandardCharsets.UTF_8));
                    }
                    System.out.println("Записано в HDFS: " + file);
                }
                consumer.commitSync();
            }
        }
    }
}
