# ---------- Stage 1: build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml /workspace/pom.xml
COPY weather-station/pom.xml /workspace/weather-station/pom.xml
COPY kafka-processor/pom.xml /workspace/kafka-processor/pom.xml
COPY central-station/pom.xml /workspace/central-station/pom.xml

RUN mvn -pl kafka-processor -am dependency:go-offline -B

COPY kafka-processor/src /workspace/kafka-processor/src
RUN mvn -pl kafka-processor -am clean package -DskipTests -B

# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/kafka-processor/target/kafka-processor.jar /app/kafka-processor.jar
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/kafka-processor.jar"]