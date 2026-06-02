import pika
import json
import time
import tensorflow as tf

# Load MNIST test data
(_, _), (x_test, y_test) = tf.keras.datasets.mnist.load_data()

# Connect to RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

# Create durable queues for three consumers
channel.queue_declare(queue="mnist_nn_queue", durable=True)
channel.queue_declare(queue="mnist_cnn_queue", durable=True)
channel.queue_declare(queue="mnist_rf_queue", durable=True)

# Send 10 MNIST images
for i in range(10):
    message = {
        "image_id": i,
        "image": x_test[i].tolist(),
        "actual_label": int(y_test[i])
    }

    body = json.dumps(message)

    # Send same image to all three model consumers
    channel.basic_publish(exchange="", routing_key="mnist_nn_queue", body=body)
    channel.basic_publish(exchange="", routing_key="mnist_cnn_queue", body=body)
    channel.basic_publish(exchange="", routing_key="mnist_rf_queue", body=body)

    print(f"Sent image {i}, actual label = {y_test[i]}")

    time.sleep(2)

connection.close()