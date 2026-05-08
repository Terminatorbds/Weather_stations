# ---------- Stage 1: build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml /workspace/pom.xml
COPY weather-station/pom.xml /workspace/weather-station/pom.xml
COPY kafka-processor/pom.xml /workspace/kafka-processor/pom.xml
COPY central-station/pom.xml /workspace/central-station/pom.xml

RUN mvn -pl central-station -am dependency:go-offline -B

COPY central-station/src /workspace/central-station/src
RUN mvn -pl central-station -am clean package -DskipTests -B

# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/central-station/target/central-station.jar /app/central-station.jar
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/central-station.jar"]