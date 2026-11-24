package org.example.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.common.KafkaSettings;
import org.example.common.Topics;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Потоковая фильтрация: товары из shop-products, которых нет в banned-products, уходят в allowed-products.
 * Список запрещённых хранится в глобальном state store, который наполняется из compacted-топика.
 */
public class BannedProductsFilterApp {

    private static final String BANNED_STORE = "banned-products-store";

    public static void main(String[] args) throws InterruptedException {
        Properties props = KafkaSettings.clientProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-filter-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        StoreBuilder<KeyValueStore<String, String>> bannedStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(BANNED_STORE), Serdes.String(), Serdes.String()).withLoggingDisabled();

        Topology topology = new Topology();
        topology.addGlobalStore(bannedStore, "BannedSource",
                Serdes.String().deserializer(), Serdes.String().deserializer(),
                Topics.BANNED_PRODUCTS, "BannedUpdater", BannedUpdater::new);
        topology.addSource("ShopSource", Topics.SHOP_PRODUCTS);
        topology.addProcessor("FilterProcessor", FilterProcessor::new, "ShopSource");
        topology.addSink("AllowedSink", Topics.ALLOWED_PRODUCTS, "FilterProcessor");

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
        System.out.println("Фильтр запрещённых товаров запущен");
        latch.await();
    }

    public static class BannedUpdater implements Processor<String, String, Void, Void> {
        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            store = context.getStateStore(BANNED_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.value() == null) {
                store.delete(record.key());
            } else {
                store.put(record.key(), record.value());
            }
        }
    }

    public static class FilterProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> banned;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.banned = context.getStateStore(BANNED_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.value() == null) {
                return;
            }
            if (banned.get(record.key()) != null) {
                System.out.println("Запрещённый товар отфильтрован: " + record.key());
                return;
            }
            context.forward(record);
        }
    }
}
