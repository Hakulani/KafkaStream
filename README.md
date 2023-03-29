# KafkaStream
DADS6005 Kafka Stream

docker compose up -d
curl -d @"source.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
curl -d @"source-sink.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors

./gradlew build



./gradlew runStreams -Pargs=basic


