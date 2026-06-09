import pika
import json
from collections import defaultdict

RABBITMQ_HOST = "localhost"
OUTPUT_QUEUE = "translation_result_queue"

EXPECTED_LANGUAGES = {"Telugu", "Tamil", "Malayalam", "Hindi"}

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

results = defaultdict(dict)

print("\n================ AGGREGATOR STARTED ================")
print("Waiting for translated messages...\n")

def print_full_chunk(chunk_id):
    chunk_results = results[chunk_id]

    print("\n" + "=" * 90)
    print(f"FINAL MULTILINGUAL OUTPUT FOR VIDEO CHUNK {chunk_id}")
    print("=" * 90)

    any_lang = next(iter(chunk_results.values()))
    print("Original English:")
    print(any_lang["original_text"])
    print("\nSemantic topic detected:", any_lang["semantic_topic"])
    print("NLP feature:", any_lang["nlp_feature"])

    for lang in ["Telugu", "Tamil", "Malayalam", "Hindi"]:
        if lang in chunk_results:
            print(f"\n{lang}:")
            print(chunk_results[lang]["translated_text"])

    print("=" * 90 + "\n")

def callback(ch, method, properties, body):
    message = json.loads(body)

    chunk_id = message["chunk_id"]
    lang = message["target_language"]

    results[chunk_id][lang] = message

    print(f"Aggregator received chunk {chunk_id} translation in {lang}")

    if EXPECTED_LANGUAGES.issubset(results[chunk_id].keys()):
        print_full_chunk(chunk_id)

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=OUTPUT_QUEUE, on_message_callback=callback)
channel.start_consuming()
