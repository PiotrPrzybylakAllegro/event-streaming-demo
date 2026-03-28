# Kafka Microservices Demo

Two Spring Boot services communicating via Kafka.

## Prerequisites
- Docker & Docker Compose
- JDK 17
- Maven 3.9+

## Start Kafka locally
```bash
docker-compose up -d
```

## Run services
In separate terminals from the project root:
```bash
./mvnw -pl service-a spring-boot:run
./mvnw -pl service-b spring-boot:run
```

## Send a message
```bash
curl -X POST http://localhost:8081/messages \
  -H "Content-Type: application/json" \
  -d '{"id":"1","content":"hello from service A"}'
```
Service B logs the consumed message on port 8082.
