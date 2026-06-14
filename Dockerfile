# syntax=docker/dockerfile:1
FROM eclipse-temurin:25-jdk-jammy AS build
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends maven \
    && rm -rf /var/lib/apt/lists/*

COPY pom.xml ./
COPY src ./src/

RUN mvn -B -DskipTests package

FROM eclipse-temurin:25-jre-jammy
WORKDIR /app

COPY --from=build /app/target/transaction-service-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
