FROM confluentinc/cp-kafka-connect:7.4.4

# JMX Exporter для Kafka Connect
COPY monitoring/jmx/jmx_prometheus_javaagent-0.15.0.jar /opt/jmx/
COPY monitoring/jmx/kafka-connect.yml /opt/jmx/

# FileStreamSinkConnector (connect-file) отсутствует в образе cp-kafka-connect
COPY connect/plugins/connect-file-3.7.0.jar /usr/share/confluent-hub-components/
