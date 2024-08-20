import psycopg2
from kafka import KafkaProducer, KafkaConsumer
import json

# Postgres connection
db_host = "192.168.3.66"
db_name = "postgres"
db_user = "ps"
db_password = "ps"
db_port = "5432"

# Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Kafka consumer
consumer = KafkaConsumer('profit_loss_data', bootstrap_servers='localhost:9092')

# Produce data to Kafka
def produce_data():
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM profit_loss_data")
    rows = cur.fetchall()
    for row in rows:
        data = {
            "Sales": row[0],
            "Expenses": row[1],
            "Operating Profit": row[2],
            "OPM %": row[3],
            "Other Income": row[4],
            "Interest": row[5],
            "Depreciation": row[6],
            "Profit before tax": row[7],
            "Tax %": row[8],
            "Net Profit": row[9],
            "EPS in Rs": row[10],
            "Dividend Payout %": row[11]
        }
        producer.send('profit_loss_data', value=json.dumps(data).encode('utf-8'))

# Consume data from Kafka and write to file
def consume_data():
    with open('consumer_output.json', 'w') as f:
        for message in consumer:
            data = json.loads(message.value.decode('utf-8'))
            json.dump(data, f)
            f.write('\n')

if __name__ == '__main__':
    produce_data()
    consume_data()
