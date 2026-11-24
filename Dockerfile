# ЭТАП 1: сборка проекта
FROM maven:3.8-openjdk-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q clean package -DskipTests

# ЭТАП 2: запуск приложений (один jar на все классы)
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/target/kafka_final-1.0-SNAPSHOT-jar-with-dependencies.jar /app/app.jar
