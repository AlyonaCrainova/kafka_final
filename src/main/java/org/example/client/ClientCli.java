package org.example.client;

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
import java.util.List;
import java.util.Properties;

/**
 * CLIENT API: команды, которые вводятся в терминале.
 * <ul>
 *   <li>search &lt;user_id&gt; &lt;запрос&gt; — поиск в файле разрешённых товаров, поисковый запрос отправляется в user-events (producer)</li>
 *   <li>recommend &lt;user_id&gt; — готовые рекомендации из топика recommendations (только consumer)</li>
 *   <li>view|cart|buy|review &lt;user_id&gt; &lt;product_id&gt; — события взаимодействия в user-events</li>
 * </ul>
 */
public class ClientCli {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            return;
        }
        String command = args[0];
        String userId = args[1];

        switch (command) {
            case "search" -> {
                if (args.length < 3) {
                    printUsage();
                    return;
                }
                search(userId, String.join(" ", Arrays.copyOfRange(args, 2, args.length)));
            }
            case "recommend" -> recommend(userId);
            case "view", "cart", "buy", "review" -> {
                if (args.length < 3) {
                    printUsage();
                    return;
                }
                interaction(command, userId, args[2]);
            }
            default -> printUsage();
        }
    }

    private static void search(String userId, String query) throws Exception {
        if (!ProductSearch.catalogExists()) {
            System.out.println("Каталог пуст: сначала загрузите товары (shop-producer)");
        }
        List<String> found = ProductSearch.search(query);
        System.out.println("Найдено товаров: " + found.size());
        found.forEach(System.out::println);

        ObjectNode event = baseEvent("search", userId);
        event.put("query", query);
        event.put("found", found.size());
        sendEvent(userId, event);
    }

    private static void recommend(String userId) {
        String recommendation = TopicSnapshot.latestValues(Topics.RECOMMENDATIONS).get(userId);
        if (recommendation == null) {
            System.out.println("Рекомендаций для пользователя " + userId + " пока нет");
        } else {
            System.out.println(recommendation);
        }
    }

    private static void interaction(String type, String userId, String productId) throws Exception {
        ObjectNode event = baseEvent(type, userId);
        event.put("product_id", productId);
        sendEvent(userId, event);
        System.out.println("Событие " + type + " для товара " + productId + " отправлено");
    }

    private static ObjectNode baseEvent(String type, String userId) {
        ObjectNode event = Json.MAPPER.createObjectNode();
        event.put("event_type", type);
        event.put("user_id", userId);
        event.put("timestamp", Instant.now().toString());
        return event;
    }

    private static void sendEvent(String userId, ObjectNode event) throws Exception {
        Properties props = KafkaSettings.clientProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(Topics.USER_EVENTS, userId,
                    Json.MAPPER.writeValueAsString(event))).get();
        }
    }

    private static void printUsage() {
        System.out.println("Использование:");
        System.out.println("  client-cli search <user_id> <запрос>");
        System.out.println("  client-cli recommend <user_id>");
        System.out.println("  client-cli view|cart|buy|review <user_id> <product_id>");
    }
}
