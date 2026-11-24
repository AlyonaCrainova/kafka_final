package org.example.admin;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.common.Json;
import org.example.common.KafkaSettings;
import org.example.common.Topics;
import org.example.common.TopicSnapshot;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * CLI модератора: управление списком запрещённых товаров (топик banned-products).
 * Команды: add <product_id> [название] | remove <product_id> | list
 */
public class BannedProductsCli {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "add" -> {
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                String name = args.length > 2 ? String.join(" ", Arrays.copyOfRange(args, 2, args.length)) : "";
                ObjectNode value = Json.MAPPER.createObjectNode();
                value.put("product_id", args[1]);
                value.put("name", name);
                value.put("banned_at", Instant.now().toString());
                send(args[1], Json.MAPPER.writeValueAsString(value));
                System.out.println("Товар " + args[1] + " добавлен в список запрещённых");
            }
            case "remove" -> {
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                send(args[1], null);
                System.out.println("Товар " + args[1] + " удалён из списка запрещённых");
            }
            case "list" -> {
                Map<String, String> banned = TopicSnapshot.latestValues(Topics.BANNED_PRODUCTS);
                if (banned.isEmpty()) {
                    System.out.println("Список запрещённых товаров пуст");
                }
                banned.forEach((id, value) -> System.out.println(id + " -> " + value));
            }
            default -> printUsage();
        }
    }

    private static void send(String productId, String value) throws Exception {
        Properties props = KafkaSettings.clientProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(Topics.BANNED_PRODUCTS, productId, value)).get();
        }
    }

    private static void printUsage() {
        System.out.println("Использование:");
        System.out.println("  banned-cli add <product_id> [название]");
        System.out.println("  banned-cli remove <product_id>");
        System.out.println("  banned-cli list");
    }
}
