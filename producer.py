import psycopg2
from kafka import KafkaProducer

# Postgres connection
db_host = "192.168.3.66"
db_name = "postgres"
db_user = "ps"
db_password = "ps"
db_port = "5432"

# Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Query data from Postgres
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

# Produce data to Kafka
for row in rows:
    producer.send('profit_loss_data', value=row)

# Close connections
cur.close()
conn.close()
producer.close()