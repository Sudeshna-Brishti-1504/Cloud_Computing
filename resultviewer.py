import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(
    queue="final_prediction_queue",
    durable=True
)

def callback(ch, method, properties, body):

    message = json.loads(body)

    print("\n====================================")
    print("FINAL PREDICTION RESULT")
    print("====================================")
    print("Image ID:", message["image_id"])
    print("Votes:", message["votes"])
    print("Final Prediction:", message["final_prediction"])
    print("Actual Label:", message["actual"])
    print("Result:", message["result"])

    print("\nModel-wise Predictions:")
    for item in message["model_predictions"]:
        print(item["model"], "predicted", item["prediction"])

    print("====================================\n")

channel.basic_consume(
    queue="final_prediction_queue",
    on_message_callback=callback,
    auto_ack=True
)

print("Final Result Viewer waiting...")

channel.start_consuming()