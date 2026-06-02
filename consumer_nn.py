import pika
import json
import numpy as np
import tensorflow as tf

# Load trained NN model
model = tf.keras.models.load_model("nn_model.keras")

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(queue="mnist_nn_queue", durable=True)

def callback(ch, method, properties, body):
    message = json.loads(body)

    image = np.array(message["image"]) / 255.0
    actual = message["actual_label"]
    image_id = message["image_id"]

    image = np.expand_dims(image, axis=0)

    prediction = model.predict(image, verbose=0)
    predicted = int(np.argmax(prediction))

    print("NN Consumer")
    print("Image ID:", image_id)
    print("Actual:", actual)
    print("Predicted:", predicted)
    print("----------------------")

channel.basic_consume(
    queue="mnist_nn_queue",
    on_message_callback=callback,
    auto_ack=True
)

print("NN Consumer waiting...")
channel.start_consuming()