import pika
import json
import time
import tensorflow as tf

# Load MNIST test dataset
(_, _), (x_test, y_test) = tf.keras.datasets.mnist.load_data()

# Connect to RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

# Create queues
channel.queue_declare(
    queue="mnist_nn_queue",
    durable=True
)

channel.queue_declare(
    queue="mnist_cnn_queue",
    durable=True
)

channel.queue_declare(
    queue="mnist_rf_queue",
    durable=True
)

print("Producer Started...")

# Send first 10 images
for i in range(10):

    image = x_test[i].tolist()

    actual_label = int(y_test[i])

    message = {
        "image_id": i,
        "image": image,
        "actual_label": actual_label
    }

    body = json.dumps(message)

    # Send same image to NN
    channel.basic_publish(
        exchange="",
        routing_key="mnist_nn_queue",
        body=body
    )

    # Send same image to CNN
    channel.basic_publish(
        exchange="",
        routing_key="mnist_cnn_queue",
        body=body
    )

    # Send same image to RF
    channel.basic_publish(
        exchange="",
        routing_key="mnist_rf_queue",
        body=body
    )

    print(
        f"Image {i} sent "
        f"(Actual Label = {actual_label})"
    )

    time.sleep(2)

connection.close()

print("Producer Finished.")