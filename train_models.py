import tensorflow as tf
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib

# Load MNIST data
(x_train, y_train), (x_test, y_test) = tf.keras.datasets.mnist.load_data()

# Normalize images for deep learning models
x_train_dl = x_train / 255.0
x_test_dl = x_test / 255.0

# -----------------------------
# Model 1: Simple Neural Network
# -----------------------------
nn_model = tf.keras.Sequential([
    tf.keras.layers.Flatten(input_shape=(28, 28)),
    tf.keras.layers.Dense(128, activation="relu"),
    tf.keras.layers.Dense(10, activation="softmax")
])

nn_model.compile(
    optimizer="adam",
    loss="sparse_categorical_crossentropy",
    metrics=["accuracy"]
)

print("Training Neural Network...")
nn_model.fit(x_train_dl, y_train, epochs=2, validation_data=(x_test_dl, y_test))
nn_model.save("nn_model.keras")


# -----------------------------
# Model 2: CNN Model
# -----------------------------
x_train_cnn = x_train_dl.reshape(-1, 28, 28, 1)
x_test_cnn = x_test_dl.reshape(-1, 28, 28, 1)

cnn_model = tf.keras.Sequential([
    tf.keras.layers.Conv2D(16, (3, 3), activation="relu", input_shape=(28, 28, 1)),
    tf.keras.layers.MaxPooling2D((2, 2)),
    tf.keras.layers.Flatten(),
    tf.keras.layers.Dense(64, activation="relu"),
    tf.keras.layers.Dense(10, activation="softmax")
])

cnn_model.compile(
    optimizer="adam",
    loss="sparse_categorical_crossentropy",
    metrics=["accuracy"]
)

print("Training CNN...")
cnn_model.fit(x_train_cnn, y_train, epochs=2, validation_data=(x_test_cnn, y_test))
cnn_model.save("cnn_model.keras")


# -----------------------------
# Model 3: Random Forest
# -----------------------------
print("Training Random Forest...")

x_train_rf = x_train.reshape(len(x_train), 28 * 28) / 255.0
x_test_rf = x_test.reshape(len(x_test), 28 * 28) / 255.0

# Use smaller data for fast demo
rf_model = RandomForestClassifier(n_estimators=50)

rf_model.fit(x_train_rf[:5000], y_train[:5000])

acc = rf_model.score(x_test_rf[:1000], y_test[:1000])
print("Random Forest Accuracy:", acc)

joblib.dump(rf_model, "rf_model.pkl")

print("All models trained and saved.")