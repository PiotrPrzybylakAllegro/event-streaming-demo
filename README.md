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
BFF publishes a `CommandEnvelope` with type `campaign-command` to `ad-manager-events`.
Service A consumes, validates, and emits an `EventEnvelope` with type `campaign-event` back to `ad-manager-events`.
BFF consumes `EventEnvelope` records to build its read model and serves `GET /campaigns`.

## Add an ad group via BFF
```bash
curl -X POST http://localhost:8080/campaigns/c1/adgroups \
  -H "Content-Type: application/json" \
  -d '{"id":"g1","campaignId":"c1","name":"adgroup","budget":200,"startDate":"2026-04-01","endDate":"2026-04-30"}'
```
Flow on the single topic `ad-manager-events`:
- BFF publishes a `CommandEnvelope` with type `adgroup-command` (payload includes the path campaignId).
- Service A consumes, checks campaign existence from in-memory state built from `campaign-event` envelopes, validates fields, and emits an `EventEnvelope` with type `adgroup-event` (APPROVED/REJECTED + reason).
- BFF consumes `adgroup-event` envelopes to populate ad groups under each campaign. Consumers seek to beginning on assignment to rebuild state after restarts.
- Docker-compose initializes only `ad-manager-events` with `retention.ms=-1` (infinite retention).


podman run -d --name kafbat -p 8085:8080 \
-e KAFKA_CLUSTERS_0_NAME=local \
-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.containers.internal:29092 \
provectuslabs/kafka-ui:latest
