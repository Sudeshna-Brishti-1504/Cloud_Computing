import pika

RABBITMQ_HOST = "localhost"
queues = ["video_stream_queue", "translation_result_queue"]

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

for q in queues:
    channel.queue_declare(queue=q, durable=True)
    channel.queue_purge(queue=q)
    print("Purged:", q)

connection.close()
