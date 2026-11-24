#!/bin/bash
# Инициализация кластера: ожидание брокеров, топики (RF=3, min.insync.replicas=2) и ACL.
# CLUSTER=main — основной кластер (kafka-0..2), CLUSTER=second — второй кластер (kafka-3..5).
set -euo pipefail

CFG=/etc/kafka/client/client.properties
if [ "${CLUSTER:-main}" = "second" ]; then
  BOOT=kafka-3:9093,kafka-4:9093,kafka-5:9093
else
  CLUSTER=main
  BOOT=kafka-0:9093,kafka-1:9093,kafka-2:9093
fi

echo "Ожидание брокеров кластера $CLUSTER..."
until [ "$(kafka-broker-api-versions --bootstrap-server "$BOOT" --command-config "$CFG" 2>/dev/null | grep -c 'id:')" -ge 3 ]; do
  sleep 3
done

TOPICS="kafka-topics --bootstrap-server $BOOT --command-config $CFG"
ACL="kafka-acls --bootstrap-server $BOOT --command-config $CFG --add"

echo "Создание топиков..."
$TOPICS --create --if-not-exists --topic shop-products --partitions 3 --replication-factor 3 --config min.insync.replicas=2
$TOPICS --create --if-not-exists --topic user-events --partitions 3 --replication-factor 3 --config min.insync.replicas=2
$TOPICS --create --if-not-exists --topic allowed-products --partitions 3 --replication-factor 3 --config min.insync.replicas=2
$TOPICS --create --if-not-exists --topic recommendations --partitions 3 --replication-factor 3 --config min.insync.replicas=2

if [ "$CLUSTER" = "main" ]; then
  $TOPICS --create --if-not-exists --topic banned-products --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact
  # Внутренние топики Kafka Connect и Schema Registry создаются заранее: у их пользователей нет права Create
  $TOPICS --create --if-not-exists --topic connect-config-storage --partitions 1 --replication-factor 3 --config cleanup.policy=compact
  $TOPICS --create --if-not-exists --topic connect-offset-storage --partitions 3 --replication-factor 3 --config cleanup.policy=compact
  $TOPICS --create --if-not-exists --topic connect-status-storage --partitions 3 --replication-factor 3 --config cleanup.policy=compact
  $TOPICS --create --if-not-exists --topic _schemas --partitions 1 --replication-factor 3 --config cleanup.policy=compact
fi

echo "Выдача ACL..."
if [ "$CLUSTER" = "main" ]; then
  # shop: запись товаров из SHOP API
  $ACL --allow-principal User:shop --operation Write --operation Describe --topic shop-products

  # client: запись событий поиска и взаимодействий; чтение готовых рекомендаций
  $ACL --allow-principal User:client --operation Write --operation Describe --topic user-events
  $ACL --allow-principal User:client --operation Read --operation Describe --topic recommendations

  # moderator: управление списком запрещённых товаров
  $ACL --allow-principal User:moderator --operation Read --operation Write --operation Describe --topic banned-products

  # streams: чтение товаров и списка запрещённых, запись разрешённых
  $ACL --allow-principal User:streams --operation Read --operation Describe --topic shop-products
  $ACL --allow-principal User:streams --operation Read --operation Describe --topic banned-products
  $ACL --allow-principal User:streams --operation Write --operation Describe --topic allowed-products
  $ACL --allow-principal User:streams --operation Read --group product-filter-app

  # connect: чтение разрешённых товаров, служебные топики и группы connect-*
  $ACL --allow-principal User:connect --operation Read --operation Describe --topic allowed-products
  $ACL --allow-principal User:connect --operation Read --operation Write --operation Describe --topic connect- --resource-pattern-type prefixed
  $ACL --allow-principal User:connect --operation Read --group connect- --resource-pattern-type prefixed

  # mirror: копирование main -> second (чтение данных) и second -> main (запись рекомендаций)
  $ACL --allow-principal User:mirror --operation Read --operation Describe --topic shop-products
  $ACL --allow-principal User:mirror --operation Read --operation Describe --topic user-events
  $ACL --allow-principal User:mirror --operation Read --operation Describe --topic allowed-products
  $ACL --allow-principal User:mirror --operation Read --group mirror-main-to-second
  $ACL --allow-principal User:mirror --operation Write --operation Describe --topic recommendations
else
  # mirror: запись копий данных из main; чтение рекомендаций для возврата в main
  $ACL --allow-principal User:mirror --operation Write --operation Describe --topic shop-products
  $ACL --allow-principal User:mirror --operation Write --operation Describe --topic user-events
  $ACL --allow-principal User:mirror --operation Write --operation Describe --topic allowed-products
  $ACL --allow-principal User:mirror --operation Read --operation Describe --topic recommendations
  $ACL --allow-principal User:mirror --operation Read --group mirror-second-to-main

  # analytics: чтение данных для HDFS и запись рекомендаций
  $ACL --allow-principal User:analytics --operation Read --operation Describe --topic user-events
  $ACL --allow-principal User:analytics --operation Read --operation Describe --topic allowed-products
  $ACL --allow-principal User:analytics --operation Read --group analytics-hdfs-loader
  $ACL --allow-principal User:analytics --operation Write --operation Describe --topic recommendations
fi

echo "Инициализация кластера $CLUSTER завершена."
