import sqlite3
import paho.mqtt.client as mqtt
import json
import threading
import time
import matplotlib.pyplot as plt

# === Settings ===
MQTT_BROKER = "localhost"
TOPIC = "pico/data"
DB_PATH = "/home/admin/mqtt_data.db"

# === Database connection with threading lock ===
db_lock = threading.Lock()
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# === MQTT Subscriber Function ===
def mqtt_subscriber():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("[MQTT] Connected successfully!")
            client.subscribe(TOPIC)
        else:
            print(f"[MQTT] Connection failed with code {rc}")
    
    # Define allowed sensorIDs
    ALLOWED_SENSOR_ID = {"Team01", "Team02", "Team03", "Team04", "Team05", "Team06", "Team07", "Team08", "Team09", "Team10"}  # <-- Add your students' names here
    def on_message(client, userdata, message):
        try:
            payload = message.payload.decode('utf-8')
            data = json.loads(payload)

            # --- Validate payload structure ---
            if not isinstance(data, dict):
                print(f"[ERROR] Invalid payload format: not a JSON object")
                return

            sensor = data.get("sensorID")
            temp = data.get("temperatureReading")

            # Validate sensorID
            if not isinstance(sensor, str) or not sensor.strip():
                print(f"[ERROR] Invalid or missing sensorID: {sensor}")
                return
            
            # Enforce whitelist
            if sensor not in ALLOWED_SENSOR_ID:
                print(f"[ERROR] Unauthorized sensorID: {sensor}")
                return

            # Validate temperatureReading
            try:
                temp = float(temp)
            except (ValueError, TypeError):
                print(f"[ERROR] Invalid temperature value from {sensor}: {temp}")
                return

            # Optional: sanity check on temp value range
            if not (0 <= temp <= 100):
                print(f"[WARNING] Unusual temperature value from {sensor}: {temp}")
                return # don't add this unusual temp value to database

            # --- If all validations pass ---
            print(f"[MQTT] Valid message received from {sensor}: {temp:.2f}°C")

            with db_lock:
                cursor.execute(
                    "INSERT INTO temperatureData (sensorID, temperatureReading) VALUES (?, ?)",
                    (sensor, temp)
                )
                conn.commit()

        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON decode error: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
        

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_BROKER, 1883, keepalive=60)
            client.loop_forever()
        except Exception as e:
            print(f"[MQTT] Connection error: {e}")
            time.sleep(5)  # Retry after delay

# === Start Subscriber Thread ===
subscriber_thread = threading.Thread(target=mqtt_subscriber, daemon=True)
subscriber_thread.start()

# === Dashboard in Main Thread ===
plt.ion()
fig, ax = plt.subplots()

try:
    while True:
        with db_lock:
            cursor.execute("""
                SELECT sensorID, temperatureReading
                FROM temperatureData
                WHERE (sensorID, timestamp) IN (
                    SELECT sensorID, MAX(timestamp)
                    FROM temperatureData
                    GROUP BY sensorID
                )
            """)
            data = cursor.fetchall()

        sensors = [row[0] for row in data]
        temps = []

        for row in data:
            try:
                temps.append(float(row[1]))
            except (TypeError, ValueError):
                temps.append(0.0)  # or handle more gracefully if you want

        ax.clear()
        ax.bar(sensors, temps, color='blue')
        ax.set_title("Live Temperature Readings")
        ax.set_xlabel("sensor ID")
        ax.set_ylabel("Temperature [°C]")
        ax.set_ylim(50, 90)  # static range

        plt.draw()
        plt.pause(5)

except KeyboardInterrupt:
    print("\n[INFO] Shutting down...")
    conn.close()
    print("[INFO] Database connection closed.")
