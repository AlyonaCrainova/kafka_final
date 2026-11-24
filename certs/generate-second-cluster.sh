#!/bin/bash
# Генерирует сертификаты брокеров второго кластера (kafka-3..kafka-5),
# подписанные тем же CA, что и сертификаты основного кластера (module7).
# Запуск: см. README.md, раздел "Сертификаты".
set -euo pipefail

PASS=551288d
CA_CRT=certs/ca/ca.crt
CA_KEY=certs/ca/ca.key
CA_SRL=certs/ca/ca.srl

for N in 3 4 5; do
  DIR=kafka-$N-creds
  mkdir -p "$DIR"

  cat > "$DIR/kafka-$N.cnf" <<EOF
[req]
prompt = no
distinguished_name = dn
req_extensions = v3_req

[dn]
C = RU
O = Yandex
OU = Practice
L = Moscow
CN = kafka-$N

[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = kafka-$N
DNS.2 = localhost
EOF

  openssl req -new -newkey rsa:2048 -nodes \
    -keyout "$DIR/kafka-$N.key" -out "$DIR/kafka-$N.csr" -config "$DIR/kafka-$N.cnf"
  openssl x509 -req -in "$DIR/kafka-$N.csr" \
    -CA "$CA_CRT" -CAkey "$CA_KEY" -CAserial "$CA_SRL" \
    -out "$DIR/kafka-$N.crt" -days 3650 -extensions v3_req -extfile "$DIR/kafka-$N.cnf"
  openssl pkcs12 -export -in "$DIR/kafka-$N.crt" -inkey "$DIR/kafka-$N.key" \
    -certfile "$CA_CRT" -name "kafka-$N" -out "$DIR/kafka-$N.p12" -passout "pass:$PASS"
  keytool -importkeystore -noprompt \
    -srckeystore "$DIR/kafka-$N.p12" -srcstoretype PKCS12 -srcstorepass "$PASS" \
    -destkeystore "$DIR/kafka-$N.keystore.jks" -deststoretype JKS -deststorepass "$PASS"

  cp security/truststore.jks "$DIR/kafka.kafka-$N.truststore.jks"
  for F in keystore sslkey truststore; do
    printf '%s' "$PASS" > "$DIR/kafka-${N}_${F}_creds"
  done
  rm -f "$DIR/kafka-$N.csr" "$DIR/kafka-$N.p12"
  echo "Сертификат kafka-$N готов"
done
