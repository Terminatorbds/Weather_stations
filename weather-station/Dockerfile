# ---------- Stage 1: build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# Copy parent POM and ALL child POMs (parent references all 3 modules)
COPY pom.xml /workspace/pom.xml
COPY weather-station/pom.xml /workspace/weather-station/pom.xml
COPY kafka-processor/pom.xml /workspace/kafka-processor/pom.xml
COPY central-station/pom.xml /workspace/central-station/pom.xml

# Pre-fetch dependencies for just our module (cached layer)
RUN mvn -pl weather-station -am dependency:go-offline -B

# Now copy ONLY this service's sources and build
COPY weather-station/src /workspace/weather-station/src
RUN mvn -pl weather-station -am clean package -DskipTests -B

# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:17-jre
WORKDIR /app

COPY --from=build /workspace/weather-station/target/weather-station.jar /app/weather-station.jar

ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/weather-station.jar"]