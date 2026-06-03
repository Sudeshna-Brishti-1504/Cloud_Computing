import pika
import json
import numpy as np
import tensorflow as tf

model = tf.keras.models.load_model("nn_model.keras")

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(queue="mnist_nn_queue", durable=True)
channel.queue_declare(queue="prediction_queue", durable=True)

def callback(ch, method, properties, body):
    message = json.loads(body)

    image_id = message["image_id"]
    actual = message["actual_label"]

    image = np.array(message["image"]) / 255.0
    image = np.expand_dims(image, axis=0)

    prediction = model.predict(image, verbose=0)
    predicted = int(np.argmax(prediction))

    print("NN Consumer")
    print("Image ID:", image_id)
    print("Actual:", actual)
    print("Predicted:", predicted)
    print("----------------------")

    prediction_message = {
        "image_id": image_id,
        "model": "NN",
        "prediction": predicted,
        "actual": actual
    }

    channel.basic_publish(
        exchange="",
        routing_key="prediction_queue",
        body=json.dumps(prediction_message)
    )

channel.basic_consume(
    queue="mnist_nn_queue",
    on_message_callback=callback,
    auto_ack=True
)

print("NN Consumer waiting...")
channel.start_consuming()