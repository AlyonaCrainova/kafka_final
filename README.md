# Аналитическая платформа маркетплейса «Покупай выгодно»

Учебный проект на Apache Kafka. Платформа собирает данные о товарах и взаимодействиях клиентов, фильтрует запрещённые товары, переносит данные в HDFS, считает аналитику и рекомендации в Spark и отдаёт рекомендации клиентам. Всё разворачивается локально в Docker Compose.

---

## 1. Архитектура

```
SHOP API (goods.json)                                CLIENT API (client-cli)
        │ shop-products                                 │ search / view / cart / buy / review
        ▼                                               │ recommend (только чтение)
┌──────────────── Основной кластер Kafka (kafka-0..2) ───────────────┐
│ shop-products ─► Kafka Streams: product-filter ─► allowed-products │
│ banned-products (модератор, banned-cli)                            │
│ user-events ◄── client-cli (поисковые запросы и взаимодействия)    │
│ recommendations ◄── MirrorMaker (из второго кластера)              │
└──────┬───────────────────────────────┬────────────────────────────┘
       │                               │
 Kafka Connect                   MirrorMaker main → second
  └─ FileSink ─► storage/allowed-products.txt   (хранилище разрешённых товаров; поиск client-cli читает этот файл)

                                       │
                         ┌──── Второй кластер Kafka (kafka-3..5) ────┐
                         │ shop-products, user-events, allowed-products │
                         │ recommendations (запись из Producer)        │
                         └──────┬──────────────────────┬─────────────┘
                                │                      │ MirrorMaker second → main
                         hdfs-loader                recommendations-publisher
                                ▼                      ▲
                         HDFS (/data/...) ◄─► Spark (RecommendationsSparkJob)
```

Клиенты (SHOP API, CLIENT API, модератор) работают только с основным кластером. Второй кластер внутренний: в него зеркалируются данные, из него загружается HDFS и в него пишутся рекомендации. Рекомендации возвращаются в основной кластер через обратное зеркалирование.

Мониторинг (JMX Exporter → Prometheus → Grafana, правила и Alertmanager) подключён к обоим кластерам и к Kafka Connect.

Схема Schema Registry подключена к основному кластеру. Схемы JSON Schema регистрируются сервисом `schema-init` (см. раздел 6).

---

## 2. Состав стенда

| Компонент | Образ | Назначение |
|---|---|---|
| zookeeper-main, zookeeper-second | confluentinc/cp-zookeeper:7.4.4 | координация брокеров (с SASL) |
| kafka-0..2 | сборка из `kafka/Dockerfile` (cp-kafka 7.4.4) | основной кластер, SASL_SSL, ACL, RF=3, min.insync=2 |
| kafka-3..5 | та же сборка | второй кластер |
| init-main, init-second | cp-kafka 7.4.4 | создание топиков и выдача ACL (`init/init-cluster.sh`) |
| schema-registry | cp-schema-registry 7.4.4 | хранение JSON Schema |
| schema-init | сборка приложения | регистрация схем |
| kafka-connect | сборка из `connect/Dockerfile` | FileSink: выгрузка разрешённых товаров в файл |
| connect-setup | curlimages/curl | регистрация коннекторов из `connect/connectors/` |
| mirror-main-to-second, mirror-second-to-main | cp-kafka 7.4.4 | MirrorMaker (`kafka-mirror-maker`) |
| product-filter | сборка приложения | Kafka Streams, фильтр запрещённых товаров |
| hadoop-namenode, hadoop-datanode-1/2 | apache/hadoop:3.4.1 | HDFS (Data Lake) |
| hdfs-loader | сборка приложения | второй кластер → HDFS |
| spark-master, spark-worker-1 | bitnamilegacy/spark:3.5.4 | Spark для аналитики |
| jar-export | сборка приложения | копирует jar в том `app-jar` для Spark |
| kafka-ui | provectuslabs/kafka-ui | веб-интерфейс Kafka (оба кластера) |
| prometheus, alertmanager, grafana | prom/*, grafana/grafana | мониторинг и алерты |

Одноразовые команды (профили `jobs` и `cli`) запускаются вручную: продюсер SHOP API, CLI модератора, CLI клиента, публикация рекомендаций.

---

## 3. Требования к хосту

- Docker Desktop с не менее 16 ГБ RAM (рекомендуется 20 ГБ) и 4+ CPU. Лимиты памяти всех сервисов в сумме ориентировочно около 15 ГБ.
- Свободные порты на хосте: 3000 (Grafana), 8081 (Schema Registry), 8083 (Kafka Connect REST), 8085 (Kafka UI), 8090 (Spark UI), 9090 (Prometheus), 19093 (Alertmanager), 19870 (HDFS NameNode UI).
- Порты `9092–9093`, `8080`, `9870`, `9000` могут быть заняты другими учебными проектами (kafka_hadoop, kafka_spark). Здесь они переназначены, брокеры наружу не публикуются.

---

## 4. Запуск

Команды выполняются в папке проекта.

**1. Сборка и запуск стенда**

```bash
docker compose up -d --build
```

Compose сам запустит по порядку: ZooKeeper, брокеры, инициализацию топиков и ACL (`init-main`, `init-second`), Schema Registry, регистрацию схем (`schema-init`), Kafka Connect и регистрацию коннекторов (`connect-setup`), MirrorMaker, Kafka Streams, HDFS, Spark, мониторинг.

Проверка готовности:

```bash
docker compose ps -a
```

Брокеры должны быть `healthy`, а `init-main`, `init-second`, `schema-init`, `connect-setup`, `jar-export` — `Exited (0)`.

**2. Загрузка товаров SHOP API**

```bash
docker compose --profile jobs run --rm --no-deps shop-producer /data/goods.json
```

Файл `goods.json` (массив из 6 товаров) отправляется в топик `shop-products`. `shop-producer` принимает несколько файлов (каждый — массив или один объект), их можно передать аргументами, если они смонтированы в контейнер.

**3. Список запрещённых товаров (модератор)**

```bash
docker compose --profile cli run --rm --no-deps banned-cli add 12345 "Умные часы XYZ"
docker compose --profile cli run --rm --no-deps banned-cli list
docker compose --profile cli run --rm --no-deps banned-cli remove 12345
```

**4. Клиентский CLI**

```bash
docker compose --profile cli run --rm --no-deps client-cli search u1 умные часы
docker compose --profile cli run --rm --no-deps client-cli view u1 12345
docker compose --profile cli run --rm --no-deps client-cli buy u1 12345
docker compose --profile cli run --rm --no-deps client-cli recommend u1
```

Команды `view|cart|buy|review` отправляют событие в `user-events`. `search` ищет в файле `storage/allowed-products.txt` и тоже отправляет поисковый запрос в `user-events`. Поиск работает после загрузки товаров (`shop-producer`), до этого выводится сообщение «Каталог пуст». `recommend` только читает готовые рекомендации из топика `recommendations`.

**5. Аналитика Spark**

```bash
docker compose exec -e SPARK_USER=root -e HADOOP_USER_NAME=root spark-master \
  spark-submit --master spark://spark-master:7077 \
  --class org.example.analytics.RecommendationsSparkJob /shared/app.jar hdfs://hadoop-namenode:9000
```

Job читает `/data/user-events`, пишет `/data/analytics/product_stats` и `/data/recommendations`.

Логика рекомендаций простая: товары ранжируются по популярности (просмотры + корзина + покупки), пользователю выдаются до 5 самых популярных товаров, которые он ещё не смотрел. Поэтому список одинаков для большинства пользователей, а пользователь, просмотревший весь топ-5, рекомендаций не получит. 

**6. Публикация рекомендаций**

```bash
docker compose --profile jobs run --rm --no-deps recommendations-publisher
```

Producer читает `/data/recommendations` из HDFS и пишет в топик `recommendations` второго кластера. MirrorMaker `second → main` копирует его в основной кластер, и `client-cli recommend` начинает видеть результат (через 10–30 секунд).

Чтобы посмотреть файлы в HDFS:

```bash
docker compose exec hadoop-namenode hdfs dfs -ls -R /data
```

---

## 5. Проверка работы

| Что проверяем | Как | Ожидаемый результат |
|---|---|---|
| Фильтрация запрещённых | `banned-cli add 12345`, затем `shop-producer` | в логе `product-filter` строка «Запрещённый товар отфильтрован: 12345», товара нет в файле `storage/allowed-products.txt` |
| Запись в хранилище | `cat storage/allowed-products.txt` | в файле по одной строке JSON на каждый разрешённый товар |
| Коннектор | `docker compose exec kafka-connect curl -s localhost:8083/connectors/allowed-products-file-sink/status` | state `RUNNING` |
| Дублирование | `kafka-console-consumer` с `kafka-3` (второй кластер) по `user-events` | события из основного кластера есть на втором |
| HDFS | `hdfs dfs -ls -R /data` | каталоги `allowed-products`, `user-events` с файлами; после Spark-job — `analytics/product_stats` и `recommendations` |
| Рекомендации | `client-cli recommend u1` | JSON с `recommended_products` |
| ACL | клиент пишет в `shop-products` | запись не проходит (нет права Write) |
| TLS и аутентификация | `kafka-broker-api-versions` с `security.protocol=PLAINTEXT`; с неверным паролем | без TLS подключение не проходит; с неверным паролем `SaslAuthenticationException` |
| Репликация и min.insync | `kafka-topics --describe` для `shop-products`; затем `docker compose stop kafka-1`, запись с `acks=all`; затем `stop kafka-2`, та же запись | RF=3, `min.insync.replicas=2`, все реплики в `Isr`. При одном остановленном брокере запись проходит, при двух — `NOT_ENOUGH_REPLICAS` |
| Дублирование в оба кластера | сравнить `GetOffsetShell` для `shop-products`, `user-events`, `allowed-products` на `kafka-0` и `kafka-3` | количество сообщений совпадает |
| Возврат рекомендаций | `client-cli recommend u1` после публикации | рекомендации из второго кластера видны в основном (через MirrorMaker, 10–30 с) |
| Мониторинг | Prometheus `http://localhost:9090/targets` | все брокеры `up` |
| Алерт | `docker compose stop kafka-1`, подождать 60 с | алерт `KafkaBrokerDown` появляется в Alertmanager `http://localhost:19093/#/alerts` |
| Дашборд | Grafana `http://localhost:3000` (admin / admin), папка Kafka | дашборд «Kafka: обзор кластеров» |

---

## 6. Безопасность, топики и ACL

**Пользователи и права** (`security/kafka_server_jaas.conf`, ACL в `init/init-cluster.sh`):

| Пользователь | Права |
|---|---|
| admin | суперпользователь, для инициализации и Schema Registry |
| shop | Write в `shop-products` |
| client | Write в `user-events`, Read в `recommendations` |
| moderator | Read, Write в `banned-products` |
| streams | Read `shop-products`, `banned-products`; Write `allowed-products`; группа `product-filter-app` |
| connect | Read `allowed-products`; служебные топики и группы `connect-*` |
| mirror | копирование `shop-products`, `user-events`, `allowed-products` из main во second и `recommendations` из second в main |
| analytics | Read `user-events`, `allowed-products` во втором кластере; Write `recommendations` |

**Топики** (RF=3, `min.insync.replicas=2`; автосоздание выключено). Партиции: 3 у `shop-products`, `user-events`, `allowed-products`, `recommendations`, `connect-offset-storage`, `connect-status-storage`; 1 у `banned-products`, `connect-config-storage`, `_schemas`:

| Топик | Кластер | Назначение |
|---|---|---|
| shop-products | main, second | сырые товары SHOP API |
| banned-products | main | список запрещённых, `cleanup.policy=compact` |
| allowed-products | main, second | товары после фильтрации |
| user-events | main, second | поиск и взаимодействия клиентов |
| recommendations | main, second | рекомендации от Spark (second) и их копия (main) |
| connect-config/offset/status-storage, _schemas | main | служебные топики Connect и Schema Registry |

**TLS.** Все клиенты и брокеры работают по SASL_SSL. Сертификаты основного кластера взяты из модуля 7 (`kafka-0..2-creds`), для второго кластера созданы новые (`kafka-3..5-creds`) скриптом `certs/generate-second-cluster.sh`, подписанным тем же CA (`certs/ca/`). Truststore — `security/truststore.jks`.

**Сертификаты второго кластера.** CA лежит в `certs/ca/`. Чтобы создать сертификаты `kafka-3..5-creds` заново, из корня проекта выполните `bash certs/generate-second-cluster.sh` (нужны `openssl` и `keytool`). Скрипт не меняет сертификаты основного кластера.

---

## 7. Реализация

**Исходный код** (`src/main/java/org/example/`):

| Класс | Что делает |
|---|---|
| `shop.ShopProducer` | SHOP API: читает JSON-файлы с товарами и отправляет в `shop-products` (ключ — `product_id`, `acks=all`, идемпотентность) |
| `streams.BannedProductsFilterApp` | Kafka Streams (Processor API): список `banned-products` хранится в глобальном state store, товары из `shop-products` без совпадения уходят в `allowed-products` в исходном JSON товара |
| `admin.BannedProductsCli` | CLI модератора: `add`, `remove` (tombstone), `list` |
| `client.ClientCli` | CLI клиента: `search` (поиск по файлу + событие), `recommend` (только consumer), `view/cart/buy/review` (события) |
| `client.ProductSearch` | поиск товаров по имени без учёта регистра в файле `storage/allowed-products.txt`; при повторах `product_id` берётся последняя запись, не больше 10 результатов |
| `analytics.HdfsLoader` | consumer второго кластера: `user-events` и `allowed-products` пачками пишет в HDFS, оффсеты коммитятся после записи |
| `analytics.RecommendationsSparkJob` | Spark SQL: статистика по товарам (просмотры, корзина, покупки, популярность); рекомендации — до 5 самых популярных товаров, которые пользователь ещё не смотрел |
| `analytics.RecommendationsPublisher` | producer: читает рекомендации из HDFS и публикует в `recommendations` второго кластера |
| `common.SchemaRegistrar` | регистрация JSON Schema из `src/main/resources/schemas/` в Schema Registry |
| `common.TopicSnapshot` | читает топик от начала до текущего конца и возвращает последнее значение по ключу (для списка запрещённых и рекомендаций) |

Сборка: `Dockerfile` собирает один jar со всеми зависимостями (maven-shade-plugin с `ServicesResourceTransformer`, который объединяет `META-INF/services`); все приложения и Spark-job используют его. Клиентская библиотека Hadoop — 3.3.4, как в Spark 3.5.4, чтобы не конфликтовать с Hadoop внутри Spark; сервер HDFS — 3.4.1, клиент и сервер работают по протоколу HDFS 3.x.

**Коннекторы Kafka Connect** (`connect/connectors/`):
- `allowed-products-file-sink.json` — `FileStreamSinkConnector`, значения как строки, файл `storage/allowed-products.txt`;
- Ограничение FileSink: файл только дописывается. Товар, который сначала разрешили, а потом добавили в `banned-products`, останется в файле. Поиск при этом берёт последнюю запись по `product_id`.

**Мониторинг** (`monitoring/`):
- `jmx/kafka-broker.yml` и `jmx/kafka-connect.yml` — правила JMX Exporter (javaagent в брокерах и Connect, порт 7071);
- `prometheus/prometheus.yml` — цели `kafka-main`, `kafka-second`, `kafka-connect`;
- `prometheus/rules/kafka-alerts.yml` — `KafkaBrokerDown` (брокер не отвечает 20 с), `KafkaUnderReplicatedPartitions`, `KafkaNoActiveController`;
- `alertmanager/alertmanager.yml` — маршрутизация алертов; receiver `default` без интеграции, алерты видны в UI Alertmanager (порт 19093). внешние уведомления (Telegram, email) не настроены, это необязательная часть задания;
- `grafana/` — источник данных Prometheus и дашборд «Kafka: обзор кластеров» (брокеры в сети, контроллеры, недореплицированные и офлайн-партиции, входящий трафик и сообщения).

**Схемы** (`src/main/resources/schemas/`): JSON Schema для товаров (`shop-products-value`), событий (`user-events-value`) и рекомендаций (`recommendations-value`). Схемы зарегистрированы в Schema Registry и видны в Kafka UI; при записи в топики они не проверяются.

**Файлы и папки**:

```
kafka_final/
├── Dockerfile                     # сборка jar приложений
├── docker-compose.yaml            # весь стенд
├── pom.xml
├── goods.json                     # товары SHOP API (массив)
├── src/main/java/org/example/     # приложения (см. раздел 7)
├── src/main/resources/schemas/    # JSON Schema
├── init/init-cluster.sh           # топики и ACL обоих кластеров
├── security/                      # JAAS, клиентские свойства, truststore
├── certs/                         # CA и скрипт сертификатов второго кластера
├── kafka-0..5-creds/              # keystore/truststore брокеров
├── kafka/Dockerfile               # образ брокера с JMX Exporter
├── connect/                       # Dockerfile, коннекторы
├── mirror/                        # конфиги MirrorMaker (4 файла)
├── hadoop/                        # конфиги и скрипты HDFS
├── monitoring/                    # JMX, Prometheus, Alertmanager, Grafana
└── storage/                       # выгрузка FileSink (создаётся при запуске)
```

---

## 8. Использованные технологии

**Язык и сборка**

| Технология | Версия | Где используется |
|---|---|---|
| Java | 17 | приложения, Spark-job (образы `maven:3.8-openjdk-17`, `eclipse-temurin:17-jre`) |
| Maven | 3.8 | сборка jar (`pom.xml`, maven-shade-plugin 3.5.1) |

**Потоковая обработка и хранилище сообщений**

| Технология | Версия | Где используется |
|---|---|---|
| Apache Kafka (брокеры, Confluent Platform) | 7.4.4 (Kafka 3.4) | два кластера по 3 брокера, KRaft не используется, координация через ZooKeeper |
| Apache ZooKeeper | 7.4.4 (cp-zookeeper) | координация брокеров, SASL |
| Kafka clients / Kafka Streams | 3.4.1 | продюсер SHOP API, фильтр `BannedProductsFilterApp`, CLI |
| Kafka Connect (FileStreamSink) | 7.4.4, connect-file 3.7.0 | выгрузка `allowed-products` в файл |
| Kafka MirrorMaker | 7.4.4 | дублирование main ↔ second |
| Confluent Schema Registry | 7.4.4 | хранение JSON Schema |
| JSON Schema, Jackson | Jackson 2.15.2 | схемы товаров, событий, рекомендаций; сериализация JSON |
| Kafka UI (provectuslabs) | 0.7.0 | веб-интерфейс обоих кластеров |

**Безопасность**

| Технология | Где используется |
|---|---|
| SASL/PLAIN поверх SSL (SASL_SSL) | все клиенты и брокеры |
| TLS-сертификаты, собственный CA | `certs/ca/`, keystore/truststore в `kafka-*-creds/` и `security/` |
| Kafka ACL (AclAuthorizer) | права пользователей на топики и группы (`init/init-cluster.sh`) |

**Аналитика**

| Технология | Версия | Где используется |
|---|---|---|
| Apache Spark (Spark SQL, spark-core) | 3.5.4 | `RecommendationsSparkJob`: статистика товаров и рекомендации |
| Apache Hadoop (клиент HDFS) | 3.3.4 | приложения `HdfsLoader`, `RecommendationsPublisher` |
| Apache HDFS (сервер) | 3.4.1 | Data Lake: NameNode и два DataNode |

**Мониторинг**

| Технология | Версия | Где используется |
|---|---|---|
| JMX Exporter (javaagent) | 0.15.0 | метрики брокеров (порт 7071) и Kafka Connect |
| Prometheus | 2.51.2 | сбор метрик, правила алертов |
| Alertmanager | 0.27.0 | маршрутизация алертов (UI на порту 19093) |
| Grafana | 10.4.2 | дашборд «Kafka: обзор кластеров» |

**Инфраструктура**

| Технология | Где используется |
|---|---|
| Docker и Docker Compose | весь стенд, профили `jobs` и `cli` для одноразовых команд |
| Bash-скрипты | инициализация топиков и ACL, сертификаты второго кластера |

---

