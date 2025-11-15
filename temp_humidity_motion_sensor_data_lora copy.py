import paho.mqtt.client as mqtt
import json
import csv
import os
import requests
import time
from datetime import datetime, timedelta, timezone


# -----------------------------
# Configuration
# -----------------------------
broker = "eu1.cloud.thethings.network"
port = 1883
username = "bd-test-app2@ttn"
password = "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA"
device_id = "lht65n-01-temp-humidity-sensor"
THINGSPEAK_API_KEY = "Z350HBO4AC0ZVFIB"
EAT = timezone(timedelta(hours=3))  # East Africa Time = UTC+3

CSV_FILE = "sensor_data.csv"
JSON_FILE = "sensor_data.json"

# -----------------------------
# Ensure CSV and JSON files exist
# -----------------------------
if not os.path.isfile(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["Timestamp", "Battery", "Humidity", "Motion", "Temperature", "Motion_Status"])

if not os.path.isfile(JSON_FILE):
    with open(JSON_FILE, "w") as f_json:
        json.dump([], f_json)

# -----------------------------
# Function to save data to CSV and JSON
# -----------------------------
def save_data(timestamp, battery, humidity, motion, temperature, motion_status):
    # CSV
    with open(CSV_FILE, "a", newline="") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow([timestamp, battery, humidity, motion, temperature, motion_status])
    # JSON
    with open(JSON_FILE, "r+") as f_json:
        data = json.load(f_json)
        data.append({
            "timestamp": timestamp,
            "battery": battery,
            "humidity": humidity,
            "motion": motion,
            "temperature": temperature,
            "motion_status": motion_status
        })
        f_json.seek(0)
        json.dump(data, f_json, indent=4)

    # Send to ThingSpeak
    try:
        response = requests.post("https://api.thingspeak.com/update", data={
            "api_key": THINGSPEAK_API_KEY,
            "field1": battery,
            "field2": humidity,
            "field3": motion,
            "field4": temperature,
        })
        if response.status_code == 200:
            print("✅ Data sent to ThingSpeak")
        else:
            print("⚠️ ThingSpeak error:", response.status_code, response.text)
    except Exception as e:
        print("⚠️ Failed to send to ThingSpeak:", e)

# -----------------------------
# Fetch historical TTN data
# -----------------------------
def get_historical_sensor_data():
    app_id = "bd-test-app2"
    api_key = password  # Using same key as MQTT password
    url = f"https://{broker}/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"last": "12h"}  # last 12 hours

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        with open("message_history.json", "w") as f:
            f.write(response.text.strip())
        print("✅ Historical data saved to message_history.json")

        # Process historical data
        historical_rows = []
        with open("message_history.json", "r") as f_json:
            for line in f_json:
                line = line.strip()
                if not line:
                    continue
                msg = json.loads(line)
                uplink = msg.get("result", {}).get("uplink_message")
                if not uplink or "decoded_payload" not in uplink:
                    continue

                decoded = uplink["decoded_payload"]
                received_at = msg["result"]["received_at"]
                
                # Convert UTC -> EAT
                dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
                received_at_eat = dt.astimezone(EAT).strftime("%Y-%m-%d %H:%M:%S")

                battery = decoded.get("field1")
                humidity = decoded.get("field3")
                motion = decoded.get("field4")
                temperature = decoded.get("field5")
                motion_status = decoded.get("Exti_pin_level")

                historical_rows.append([received_at_eat, battery, humidity, motion, temperature, motion_status])

        # Sort by timestamp
        historical_rows.sort(key=lambda x: datetime.fromisoformat(x[0].replace("Z", "+00:00")))

        # Save historical rows
        for row in historical_rows:
            save_data(*row)
            print("Saved historical row:", row)

        print("✅ All historical data processed and sent to ThingSpeak.")
    else:
        print("Error fetching historical data:", response.status_code, response.text)

# -----------------------------
# MQTT Live Data
# -----------------------------
topic = f"v3/{username}/devices/{device_id}/up"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to TTN MQTT broker!")
        client.subscribe(topic)
    else:
        print(f"Failed to connect, return code {rc}")
        time.sleep(5*60)

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    uplink = payload.get("uplink_message", {}).get("decoded_payload", {})
    received_at = payload.get("received_at")
    if not uplink or not received_at:
        print("No decoded_payload in message")
        return

    # Convert UTC -> EAT
    dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    received_at_eat = dt.astimezone(EAT).strftime("%Y-%m-%d %H:%M:%S")

    battery = uplink.get("field1")
    humidity = uplink.get("field3")
    motion = uplink.get("field4")
    temperature = uplink.get("field5")
    motion_status = uplink.get("Exti_pin_level")

    row = [received_at_eat, battery, humidity, motion, temperature, motion_status]
    save_data(*row)
    print("Saved live row:", row)

# -----------------------------
# Run everything
# -----------------------------
get_historical_sensor_data()

client = mqtt.Client()
client.username_pw_set(username, password)
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port, 60)
client.loop_forever()
