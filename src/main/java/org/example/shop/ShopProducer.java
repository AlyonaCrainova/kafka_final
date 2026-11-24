package org.example.shop;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.common.Json;
import org.example.common.KafkaSettings;
import org.example.common.Topics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SHOP API (эмуляция): читает товары из JSON-файла и отправляет их в топик shop-products.
 */
public class ShopProducer {

    public static void main(String[] args) throws Exception {
        String[] files = args.length > 0 ? args : new String[]{"/data/goods.json"};
        List<JsonNode> products = new ArrayList<>();
        for (String file : files) {
            JsonNode root = Json.MAPPER.readTree(new File(file));
            if (root.isArray()) {
                root.forEach(products::add);
            } else {
                products.add(root);
            }
        }

        Properties props = KafkaSettings.clientProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (JsonNode product : products) {
                String productId = product.path("product_id").asText();
                producer.send(new ProducerRecord<>(Topics.SHOP_PRODUCTS, productId,
                        Json.MAPPER.writeValueAsString(product))).get();
                System.out.println("Товар отправлен в " + Topics.SHOP_PRODUCTS + ": " + productId
                        + " (" + product.path("name").asText() + ")");
            }
        }
    }
}
