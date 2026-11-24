#!/bin/bash
set -e

mkdir -p /usr/local/hadoop/hdfs/namenode
chmod -R 777 /usr/local/hadoop/hdfs/namenode

# Форматируем файловую систему только при первом запуске, чтобы не терять данные при перезапуске
if [ ! -d /usr/local/hadoop/hdfs/namenode/current ]; then
  hdfs namenode -format -nonInteractive
fi

exec "$@"
