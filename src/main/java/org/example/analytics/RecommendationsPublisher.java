package org.example.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.common.Json;
import org.example.common.KafkaSettings;
import org.example.common.Topics;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Producer: читает рекомендации, которые Spark записал в HDFS, и публикует их в топик recommendations
 * (на втором кластере, KAFKA_BOOTSTRAP_SERVERS). Ключ — user_id. Далее MirrorMaker переносит топик на основной кластер.
 */
public class RecommendationsPublisher {

    public static void main(String[] args) throws Exception {
        String hdfsUri = KafkaSettings.env("HDFS_URI", "hdfs://hadoop-namenode:9000");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        Properties props = KafkaSettings.clientProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        int published = 0;
        try (FileSystem fs = FileSystem.get(new URI(hdfsUri), conf, KafkaSettings.env("HADOOP_USER_NAME", "root"));
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            FileStatus[] parts = fs.globStatus(new Path("/data/recommendations/part-*"));
            if (parts == null || parts.length == 0) {
                throw new IllegalStateException("В /data/recommendations нет файлов. Сначала запустите Spark-job");
            }
            for (FileStatus part : parts) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(part.getPath()), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            continue;
                        }
                        JsonNode recommendation = Json.MAPPER.readTree(line);
                        producer.send(new ProducerRecord<>(Topics.RECOMMENDATIONS,
                                recommendation.path("user_id").asText(), line)).get();
                        published++;
                    }
                }
            }
        }
        System.out.println("Опубликовано рекомендаций в " + Topics.RECOMMENDATIONS + ": " + published);
    }
}
