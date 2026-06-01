import pika

print("Connecting to RabbitMQ...")

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(queue="hello", durable=True)

message = "Hello Distributed ML"

channel.basic_publish(
    exchange="",
    routing_key="hello",
    body=message
)

print("Message Sent:", message)

connection.close()
