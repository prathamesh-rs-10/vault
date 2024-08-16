from kafka import KafkaConsumer
import json

# Kafka consumer
consumer = KafkaConsumer('profit_loss_data', bootstrap_servers='localhost:9092')

# Consume data from Kafka and store in GitHub repo
while True:
    for message in consumer:
        row = message.value
        with open('data.jsonl', 'a') as f:
            f.write(json.dumps(row) + '\n')
