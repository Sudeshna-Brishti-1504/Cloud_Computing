import pika
import json
from collections import defaultdict, Counter

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(
    queue="prediction_queue",
    durable=True
)

# Store predictions
predictions = defaultdict(list)

def callback(ch, method, properties, body):

    message = json.loads(body)

    image_id = message["image_id"]
    model = message["model"]
    pred = message["prediction"]
    actual = message["actual"]

    predictions[image_id].append(pred)

    print(f"{model} predicted {pred}")

    # Wait until 3 models respond
    if len(predictions[image_id]) == 3:

        votes = predictions[image_id]

        final_prediction = Counter(votes).most_common(1)[0][0]

        print("\n========================")
        print("IMAGE :", image_id)
        print("Votes :", votes)
        print("Final :", final_prediction)
        print("Actual:", actual)

        if final_prediction == actual:
            print("CORRECT")
        else:
            print("WRONG")

        print("========================\n")

channel.basic_consume(
    queue="prediction_queue",
    on_message_callback=callback,
    auto_ack=True
)

print("Aggregator waiting...")

channel.start_consuming()