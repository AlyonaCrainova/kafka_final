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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.common.KafkaSettings;
import org.example.common.Topics;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Читает user-events и allowed-products из второго кластера и пишет пачки записей в HDFS:
 * /data/&lt;topic&gt;/p&lt;partition&gt;-&lt;firstOffset&gt;.json (одна запись на строку). После падения чтение начинается с последнего
 * закоммиченного оффсета, поэтому имя файла совпадает с незакоммиченной пачкой и повтор перезаписывает файл, а не создаёт дубль.
 * Оффсеты коммитятся после записи пачки.
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

                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    StringBuilder content = new StringBuilder();
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        content.append(record.value()).append('\n');
                    }
                    long firstOffset = partitionRecords.get(0).offset();

                    Path dir = new Path("/data/" + partition.topic());
                    fs.mkdirs(dir);
                    Path file = new Path(dir, "p" + partition.partition() + "-" + firstOffset + ".json");
                    try (FSDataOutputStream out = fs.create(file, true)) {
                        out.write(content.toString().getBytes(StandardCharsets.UTF_8));
                    }
                    System.out.println("Записано в HDFS: " + file);
                }
                consumer.commitSync();
            }
        }
    }
}
