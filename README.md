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
./mvnw -pl service-bff spring-boot:run
```

## Send a message
```bash
curl -X POST http://localhost:8081/messages \
  -H "Content-Type: application/json" \
  -d '{"id":"1","content":"hello from service A"}'
```
Service B logs the consumed message on port 8082.

## Create a campaign via BFF
```bash
curl -X POST http://localhost:8080/campaigns \
  -H "Content-Type: application/json" \
  -d '{"id":"c1","name":"spring launch","budget":1000}'
```
Service A consumes the campaign command from Kafka topic `campaign-commands`.
Service A validates it and emits a `campaign-events` record with APPROVED/REJECTED and reason.
Service BFF consumes `campaign-events` into an in-memory read model and serves `GET /campaigns`.

## Add an ad group via BFF
```bash
curl -X POST http://localhost:8080/campaigns/c1/adgroups \
  -H "Content-Type: application/json" \
  -d '{"id":"g1","campaignId":"c1","name":"adgroup","budget":200,"startDate":"2026-04-01","endDate":"2026-04-30"}'
```
Flow:
- BFF publishes to `adgroup-commands` (no local validation beyond path enrichment).
- Service A consumes, checks campaign existence from `campaign-events` state, validates fields, and emits `adgroup-events` with APPROVED/REJECTED plus reason.
- BFF consumes `adgroup-events` into its read model and serves them via `GET /campaigns` (adGroups array). Consumers start from earliest offsets on restart.


podman run -d --name kafbat -p 8085:8080 \
-e KAFKA_CLUSTERS_0_NAME=local \
-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.containers.internal:29092 \
provectuslabs/kafka-ui:latest
