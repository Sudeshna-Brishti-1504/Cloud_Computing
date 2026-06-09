import pika
import json
import time
import uuid

RABBITMQ_HOST = "localhost"

LANGUAGE_QUEUES = [
    "telugu_queue",
    "tamil_queue",
    "malayalam_queue",
    "hindi_queue"
]

dummy_video_transcript = [
    "Cloud computing allows students to run applications on remote servers.",
    "RabbitMQ receives video transcript chunks from the producer.",
    "Each consumer translates the content into a different Indian language.",
    "BERT style language models can understand sentence meaning using embeddings.",
    "The aggregator combines all translated messages for final display."
]

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST)
)

channel = connection.channel()

for q in LANGUAGE_QUEUES:
    channel.queue_declare(queue=q, durable=True)

print("\n================ PRODUCER STARTED ================")
print("Sending each video transcript chunk to all language queues...\n")

video_id = "dummy_video_cloud_class"

for i, sentence in enumerate(dummy_video_transcript, start=1):

    message = {
        "message_id": str(uuid.uuid4()),
        "video_id": video_id,
        "chunk_id": i,
        "timestamp_sec": i * 5,
        "source_language": "English",
        "text": sentence
    }

    for q in LANGUAGE_QUEUES:

        channel.basic_publish(
            exchange="",
            routing_key=q,
            body=json.dumps(message, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        print(f"Sent chunk {i} to {q}: {sentence}")

    print("-" * 80)
    time.sleep(2)

connection.close()

print("\nProducer finished sending all video chunks to all language queues.")