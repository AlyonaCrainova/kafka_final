FROM confluentinc/cp-kafka:7.4.4

# JMX Exporter: javaagent отдаёт метрики брокера на порту 7071 для Prometheus
COPY monitoring/jmx/jmx_prometheus_javaagent-0.15.0.jar /opt/jmx/
COPY monitoring/jmx/kafka-broker.yml /opt/jmx/
