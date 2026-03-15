import mysql.connector
import boto3
from datetime import datetime

# Configuración de AWS
aws_region = 'us-east-1'  
sqs_queue_name = 'coffee-orders'
rds_endpoint = 'database-1.cw791jmibie0.us-east-1.rds.amazonaws.com'
rds_username = 'admin'
rds_password = 'Lu123Cas456' 
rds_database = 'database-1' 

# Conectar a SQS
sqs = boto3.resource('sqs', region_name=aws_region)
queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)

# Conectar a RDS MySQL
conn = mysql.connector.connect(
    host=rds_endpoint,
    user=rds_username,
    password=rds_password,
    database=rds_database
)
cursor = conn.cursor()

# Procesar mensajes de la cola
while True:
    messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=20)
    for message in messages:
        coffee_type, timestamp = message.body.split('|')
        timestamp = datetime.fromisoformat(timestamp)
        cursor.execute("INSERT INTO coffee_orders (timestamp, coffee_type, order_status) VALUES (%s, %s, 'created')", (timestamp, coffee_type))
        conn.commit()
        message.delete()

    if not messages:
        break

conn.close()