import pika
import json
from collections import defaultdict, Counter

# Connect to RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

# Queue where NN, CNN, RF consumers send predictions
channel.queue_declare(
    queue="prediction_queue",
    durable=True
)

# Queue where aggregator writes final voting result
channel.queue_declare(
    queue="final_prediction_queue",
    durable=True
)

# Store predictions for each image
predictions = defaultdict(list)

# Store model-wise details for each image
prediction_details = defaultdict(list)

def callback(ch, method, properties, body):

    # Read prediction message from prediction_queue
    message = json.loads(body)

    image_id = message["image_id"]
    model = message["model"]
    pred = message["prediction"]
    actual = message["actual"]

    # Store only predicted digit
    predictions[image_id].append(pred)

    # Store full model detail
    prediction_details[image_id].append({
        "model": model,
        "prediction": pred
    })

    print(f"{model} predicted {pred} for image {image_id}")

    # Wait until all 3 models respond
    if len(predictions[image_id]) == 3:

        votes = predictions[image_id]

        # Majority voting
        final_prediction = Counter(votes).most_common(1)[0][0]

        if final_prediction == actual:
            result = "CORRECT"
        else:
            result = "WRONG"

        print("\n========================")
        print("IMAGE :", image_id)
        print("Votes :", votes)
        print("Final :", final_prediction)
        print("Actual:", actual)
        print("Result:", result)
        print("========================\n")

        # Final message to write back to RabbitMQ
        final_message = {
            "image_id": image_id,
            "model_predictions": prediction_details[image_id],
            "votes": votes,
            "final_prediction": final_prediction,
            "actual": actual,
            "result": result
        }

        # Aggregator writes final result to another queue
        channel.basic_publish(
            exchange="",
            routing_key="final_prediction_queue",
            body=json.dumps(final_message)
        )

        print("Final result written to final_prediction_queue")

channel.basic_consume(
    queue="prediction_queue",
    on_message_callback=callback,
    auto_ack=True
)

print("Aggregator waiting for model predictions...")

channel.start_consuming()