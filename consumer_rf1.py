import pika
import json
import numpy as np
import joblib

model = joblib.load("rf_model.pkl")

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(queue="mnist_rf_queue", durable=True)
channel.queue_declare(queue="prediction_queue", durable=True)

def callback(ch, method, properties, body):
    message = json.loads(body)

    image_id = message["image_id"]
    actual = message["actual_label"]

    image = np.array(message["image"]) / 255.0
    image = image.reshape(1, 28 * 28)

    predicted = int(model.predict(image)[0])

    print("RF Consumer")
    print("Image ID:", image_id)
    print("Actual:", actual)
    print("Predicted:", predicted)
    print("----------------------")

    prediction_message = {
        "image_id": image_id,
        "model": "RF",
        "prediction": predicted,
        "actual": actual
    }

    channel.basic_publish(
        exchange="",
        routing_key="prediction_queue",
        body=json.dumps(prediction_message)
    )

channel.basic_consume(
    queue="mnist_rf_queue",
    on_message_callback=callback,
    auto_ack=True
)

print("RF Consumer waiting...")
channel.start_consuming()