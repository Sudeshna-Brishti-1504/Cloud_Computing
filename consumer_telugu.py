import pika
import json
import time
from translation_engine import DemoTranslator

RABBITMQ_HOST = "localhost"
INPUT_QUEUE = "telugu_queue"
OUTPUT_QUEUE = "translation_result_queue"

LANGUAGE_NAME = "Telugu"
LANGUAGE_CODE = "te"

translator = DemoTranslator()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

channel.queue_declare(queue=INPUT_QUEUE, durable=True)
channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

channel.basic_qos(prefetch_count=1)

print(f"\n================ {LANGUAGE_NAME} CONSUMER STARTED ================")
print("Waiting for video transcript chunks...\n")

def callback(ch, method, properties, body):
    message = json.loads(body)

    print(f"[{LANGUAGE_NAME}] Received chunk {message['chunk_id']}")
    print("English:", message["text"])

    result = translator.translate(message["text"], LANGUAGE_CODE)

    output_message = {
        "message_id": message["message_id"],
        "video_id": message["video_id"],
        "chunk_id": message["chunk_id"],
        "timestamp_sec": message["timestamp_sec"],
        "target_language": LANGUAGE_NAME,
        "target_language_code": LANGUAGE_CODE,
        "original_text": message["text"],
        "translated_text": result["translated_text"],
        "semantic_topic": result["semantic_topic"],
        "nlp_feature": result["nlp_feature"]
    }

    channel.basic_publish(
        exchange="",
        routing_key=OUTPUT_QUEUE,
        body=json.dumps(output_message, ensure_ascii=False),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    print(f"[{LANGUAGE_NAME}] Translation sent to aggregator:")
    print(result["translated_text"])
    print("-" * 80)

    time.sleep(1)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback)
channel.start_consuming()
