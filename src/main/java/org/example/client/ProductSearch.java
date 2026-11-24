package org.example.client;

import com.fasterxml.jackson.databind.JsonNode;
import org.example.common.Json;
import org.example.common.KafkaSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Поиск товаров в файле разрешённых товаров (выгрузка Kafka Connect FileSink).
 * Файл дописывается, поэтому при повторах по product_id берётся последняя запись.
 */
public final class ProductSearch {

    private static final int LIMIT = 10;

    private ProductSearch() {
    }

    public static Path catalogFile() {
        return Path.of(KafkaSettings.env("STORAGE_FILE", "/data/storage/allowed-products.txt"));
    }

    public static boolean catalogExists() {
        return Files.exists(catalogFile());
    }

    public static List<String> search(String query) throws IOException {
        Map<String, JsonNode> products = new LinkedHashMap<>();
        if (catalogExists()) {
            for (String line : Files.readAllLines(catalogFile(), StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode product = Json.MAPPER.readTree(line);
                products.put(product.path("product_id").asText(), product);
            }
        }

        String needle = query.toLowerCase(Locale.ROOT);
        List<String> lines = new ArrayList<>();
        for (JsonNode product : products.values()) {
            if (lines.size() >= LIMIT) {
                break;
            }
            String name = product.path("name").asText();
            if (name.toLowerCase(Locale.ROOT).contains(needle)) {
                lines.add(String.format(Locale.ROOT, "%s | %s | %s | %.2f %s",
                        product.path("product_id").asText(),
                        name,
                        product.path("brand").asText(),
                        product.path("price").path("amount").asDouble(),
                        product.path("price").path("currency").asText()));
            }
        }
        return lines;
    }
}
