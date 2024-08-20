import json
from kafka import KafkaConsumer

# Kafka consumer
consumer = KafkaConsumer('profit_loss_data', bootstrap_servers='localhost:9092')

# Consume data from Kafka and write to file
with open('consumer_output.json', 'w') as f:
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        json.dump(data, f)
        f.write('\n')
