package org.example.common;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * Параметры подключения к Kafka по SASL_SSL (PLAIN + TLS).
 * Все значения берутся из переменных окружения, которые задаёт docker-compose.yaml.
 */
public final class KafkaSettings {

    private KafkaSettings() {
    }

    public static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    public static String required(String name) {
        String value = env(name, null);
        if (value == null) {
            throw new IllegalStateException("Не задана переменная окружения " + name);
        }
        return value;
    }

    public static Properties clientProps() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, required("KAFKA_BOOTSTRAP_SERVERS"));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + required("KAFKA_USER") + "\" password=\"" + required("KAFKA_PASSWORD") + "\";");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env("SSL_TRUSTSTORE_LOCATION", "/etc/kafka/client/truststore.jks"));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, required("SSL_TRUSTSTORE_PASSWORD"));
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        return props;
    }
}
