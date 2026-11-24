package org.example.common;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Регистрирует JSON Schema в Schema Registry (subject = <топик>-value).
 * Запускается один раз при старте стенда сервисом schema-init.
 */
public class SchemaRegistrar {

    private static final Map<String, String> SUBJECTS = new LinkedHashMap<>();

    static {
        SUBJECTS.put("shop-products-value", "schemas/product.schema.json");
        SUBJECTS.put("user-events-value", "schemas/user-event.schema.json");
        SUBJECTS.put("recommendations-value", "schemas/recommendation.schema.json");
    }

    public static void main(String[] args) throws Exception {
        String registryUrl = KafkaSettings.env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        HttpClient client = HttpClient.newHttpClient();

        for (Map.Entry<String, String> subject : SUBJECTS.entrySet()) {
            String schema = readResource(subject.getValue());

            ObjectNode body = Json.MAPPER.createObjectNode();
            body.put("schemaType", "JSON");
            body.put("schema", schema);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(registryUrl + "/subjects/" + subject.getKey() + "/versions"))
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .POST(HttpRequest.BodyPublishers.ofString(Json.MAPPER.writeValueAsString(body)))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IllegalStateException("Ошибка регистрации схемы " + subject.getKey() + ": " + response.body());
            }
            System.out.println("Схема " + subject.getKey() + " зарегистрирована: " + response.body());
        }
    }

    private static String readResource(String path) throws IOException {
        try (InputStream in = SchemaRegistrar.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IOException("Не найден ресурс " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
