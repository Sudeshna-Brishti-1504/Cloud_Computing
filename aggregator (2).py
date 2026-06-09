# ---------------------------------------------------------
# Import RabbitMQ library
# This is used to connect Python program with RabbitMQ broker
# ---------------------------------------------------------
import pika

# ---------------------------------------------------------
# Import JSON library
# RabbitMQ messages are sent as JSON strings
# json.loads() converts JSON string to Python dictionary
# ---------------------------------------------------------
import json

# ---------------------------------------------------------
# defaultdict is used to store results chunk-wise
# Example:
# results[1]["Telugu"]
# results[1]["Hindi"]
# ---------------------------------------------------------
from collections import defaultdict


# =========================================================
# RabbitMQ server address
# localhost means RabbitMQ is running on same Codespace/machine
# =========================================================
RABBITMQ_HOST = "localhost"


# =========================================================
# This is the queue from which aggregator reads messages
# All language consumers send translated outputs to this queue
# =========================================================
OUTPUT_QUEUE = "translation_result_queue"


# =========================================================
# Aggregator waits until all these 4 language translations arrive
# for each video chunk
# =========================================================
EXPECTED_LANGUAGES = {
    "Telugu",
    "Tamil",
    "Malayalam",
    "Hindi"
}


# =========================================================
# Create connection to RabbitMQ broker
# =========================================================
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST
    )
)


# =========================================================
# Create communication channel
# RabbitMQ operations happen through this channel
# =========================================================
channel = connection.channel()


# =========================================================
# Declare output queue
# If queue already exists, RabbitMQ will simply use it
# durable=True means queue survives RabbitMQ restart
# =========================================================
channel.queue_declare(
    queue=OUTPUT_QUEUE,
    durable=True
)


# =========================================================
# results stores translated outputs grouped by chunk_id
#
# Example structure:
#
# results = {
#     1: {
#         "Telugu": {...},
#         "Tamil": {...},
#         "Malayalam": {...},
#         "Hindi": {...}
#     },
#     2: {
#         "Telugu": {...},
#         "Tamil": {...}
#     }
# }
#
# defaultdict(dict) automatically creates empty dictionary
# for a new chunk_id
# =========================================================
results = defaultdict(dict)


# =========================================================
# Startup message
# =========================================================
print("\n================ AGGREGATOR STARTED ================")
print("Waiting for translated messages...\n")


# =========================================================
# This function prints final multilingual output
# only after all expected languages are received
# =========================================================
def print_full_chunk(chunk_id):

    # -----------------------------------------------------
    # Get all translation results for this chunk_id
    # -----------------------------------------------------
    chunk_results = results[chunk_id]

    # -----------------------------------------------------
    # Print heading
    # -----------------------------------------------------
    print("\n" + "=" * 90)
    print(f"FINAL MULTILINGUAL OUTPUT FOR VIDEO CHUNK {chunk_id}")
    print("=" * 90)

    # -----------------------------------------------------
    # Take any one language result
    # because original English text is same in all languages
    # -----------------------------------------------------
    any_lang = next(iter(chunk_results.values()))

    # -----------------------------------------------------
    # Print original English sentence
    # -----------------------------------------------------
    print("Original English:")
    print(any_lang["original_text"])

    # -----------------------------------------------------
    # Print topic detected by translation_engine.py
    # -----------------------------------------------------
    print("\nSemantic topic detected:", any_lang["semantic_topic"])

    # -----------------------------------------------------
    # Print NLP/Translation method used
    # Example: Google Translator
    # -----------------------------------------------------
    print("NLP feature:", any_lang["nlp_feature"])

    # -----------------------------------------------------
    # Print translations in fixed order
    # -----------------------------------------------------
    for lang in ["Telugu", "Tamil", "Malayalam", "Hindi"]:

        # -------------------------------------------------
        # Check whether this language translation is present
        # -------------------------------------------------
        if lang in chunk_results:

            print(f"\n{lang}:")
            print(chunk_results[lang]["translated_text"])

    # -----------------------------------------------------
    # End separator
    # -----------------------------------------------------
    print("=" * 90 + "\n")


# =========================================================
# Callback function
#
# RabbitMQ calls this function automatically whenever
# a translated message arrives in translation_result_queue
# =========================================================
def callback(ch, method, properties, body):

    # -----------------------------------------------------
    # Convert JSON message body into Python dictionary
    # -----------------------------------------------------
    message = json.loads(body)

    # -----------------------------------------------------
    # Extract video chunk id
    # Example: chunk_id = 1, 2, 3...
    # -----------------------------------------------------
    chunk_id = message["chunk_id"]

    # -----------------------------------------------------
    # Extract target language name
    # Example: Telugu / Tamil / Malayalam / Hindi
    # -----------------------------------------------------
    lang = message["target_language"]

    # -----------------------------------------------------
    # Store this translated message
    #
    # Example:
    # results[1]["Tamil"] = message
    # -----------------------------------------------------
    results[chunk_id][lang] = message

    # -----------------------------------------------------
    # Print small status update
    # -----------------------------------------------------
    print(
        f"Aggregator received chunk {chunk_id} translation in {lang}"
    )

    # -----------------------------------------------------
    # Check whether all 4 languages are received
    #
    # results[chunk_id].keys()
    # may contain:
    # {"Telugu", "Tamil", "Malayalam", "Hindi"}
    #
    # If all languages are present, print final output
    # -----------------------------------------------------
    if EXPECTED_LANGUAGES.issubset(
        results[chunk_id].keys()
    ):
        print_full_chunk(chunk_id)

    # -----------------------------------------------------
    # Send acknowledgement to RabbitMQ
    #
    # This tells RabbitMQ:
    # "Message processed successfully."
    #
    # Then RabbitMQ removes message from queue.
    # -----------------------------------------------------
    ch.basic_ack(
        delivery_tag=method.delivery_tag
    )


# =========================================================
# Register consumer with RabbitMQ
#
# Queue:
# translation_result_queue
#
# When message arrives:
# callback() function will run
# =========================================================
channel.basic_consume(
    queue=OUTPUT_QUEUE,
    on_message_callback=callback
)


# =========================================================
# Start listening forever
#
# Aggregator keeps running and waits for translated messages
# from language consumers
# =========================================================
channel.start_consuming()