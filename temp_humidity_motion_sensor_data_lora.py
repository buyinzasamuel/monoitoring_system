import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import time
import os
import requests
import ssl

# TTN Configuration
ttn_broker = "eu1.cloud.thethings.network"
ttn_port = 1883
ttn_username = "bd-test-app2@ttn"
ttn_password = "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA"
device_id = "lht65n-01-temp-humidity-sensor"  # Updated sensor name

# AWS IoT Configuration
aws_iot_endpoint = "a160h4cq9f9zk1-ats.iot.eu-north-1.amazonaws.com"  # Replace with your AWS IoT endpoint
aws_iot_port = 8883
aws_iot_topic = f"sensors/lht65n/{device_id}/data"
# AWS IoT Configuration
#aws_iot_endpoint = "your-aws-iot-endpoint.iot.region.amazonaws.com"  # This is the placeholder


# AWS IoT Certificate paths (update these paths)
cert_path = "./certificates/device.cert.pem"
key_path = "./certificates/private.key"
root_ca_path = "./certificates/root.ca.pem"

def decode_lht65n_payload(decoded_payload):
    """
    Decode LHT65N sensor payload fields
    Field mapping based on LHT65N sensor documentation
    """
    try:
        sensor_data = {
            'device_id': device_id,
            'timestamp': datetime.utcnow().isoformat(),
            'battery_voltage': decoded_payload.get('field1'),
            'humidity': decoded_payload.get('field3'),
            'motion_counts': decoded_payload.get('field4'),
            'temperature': decoded_payload.get('field5'),
            'received_at': datetime.utcnow().isoformat()
        }
        
        # Remove None values
        sensor_data = {k: v for k, v in sensor_data.items() if v is not None}
        
        return sensor_data
    except Exception as e:
        print(f"Error decoding payload: {e}")
        return None

def store_data_locally(sensor_data):
    """Store sensor data in local JSON file (replacement for DynamoDB)"""
    try:
        filename = "sensor_data.json"
        
        # Read existing data
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = []
        
        # Append new data
        existing_data.append(sensor_data)
        
        # Keep only last 1000 records to avoid file getting too large
        if len(existing_data) > 1000:
            existing_data = existing_data[-1000:]
        
        # Write back to file
        with open(filename, 'w') as f:
            json.dump(existing_data, f, indent=2)
        
        print(f"Data stored locally: {sensor_data}")
        
    except Exception as e:
        print(f"Error storing data locally: {e}")

def publish_to_aws_iot(sensor_data):
    """Publish sensor data to AWS IoT Core"""
    try:
        # Create AWS IoT client
        aws_client = mqtt.Client()
        
        # Configure TLS
        aws_client.tls_set(
            ca_certs=root_ca_path,
            certfile=cert_path,
            keyfile=key_path,
            tls_version=ssl.PROTOCOL_TLSv1_2
        )
        
        # Connect to AWS IoT
        aws_client.connect(aws_iot_endpoint, aws_iot_port, 60)
        aws_client.loop_start()
        
        # Publish message
        result = aws_client.publish(aws_iot_topic, json.dumps(sensor_data))
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Data published to AWS IoT: {aws_iot_topic}")
        else:
            print(f"Failed to publish to AWS IoT: {result.rc}")
        
        aws_client.disconnect()
        aws_client.loop_stop()
        
    except Exception as e:
        print(f"Error publishing to AWS IoT: {e}")

def process_sensor_data(ttn_payload):
    """Process TTN payload and send to AWS"""
    try:
        print("Processing sensor data...")
        
        # Extract decoded payload from TTN message
        if 'uplink_message' in ttn_payload:
            uplink_message = ttn_payload['uplink_message']
            decoded_payload = uplink_message.get('decoded_payload', {})
            received_at = uplink_message.get('received_at', '')
            
            # Decode sensor data
            sensor_data = decode_lht65n_payload(decoded_payload)
            if sensor_data:
                sensor_data['ttn_received_at'] = received_at
                
                # Store locally (replacement for DynamoDB)
                store_data_locally(sensor_data)
                
                # Publish to AWS IoT
                publish_to_aws_iot(sensor_data)
                
                return sensor_data
        else:
            print("No uplink_message found in payload")
            
    except Exception as e:
        print(f"Error processing sensor data: {e}")
    
    return None

# Fetch Historical Data - FIXED VERSION
def get_historical_sensor_data():
    """Fetch historical data from TTN and send to AWS"""
    app_id = "bd-test-app2"
    api_key = "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA"
    url = f"https://{ttn_broker}/api/v3/as/applications/{app_id}/packages/storage/uplink_message"

    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "last": "12h"  # get messages from last 12 hours
    }
    
    try:
        print("🔄 Fetching historical data from TTN...")
        response = requests.get(url, headers=headers, params=params)
        print(f"📊 HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            # Save raw response for debugging
            with open("raw_ttn_response.txt", "w", encoding="utf-8") as f:
                f.write(response.text)
            print("💾 Raw response saved to raw_ttn_response.txt")
            
            # TTN returns NDJSON (newline-delimited JSON), not a single JSON object
            # So we need to parse each line separately
            lines = response.text.strip().split('\n')
            processed_count = 0
            error_count = 0
            
            print(f"📨 Found {len(lines)} lines in response")
            
            for i, line in enumerate(lines):
                if line.strip():  # Skip empty lines
                    try:
                        # Parse each line as individual JSON
                        message = json.loads(line.strip())
                        
                        # Check if this message contains our device data
                        if 'result' in message:
                            device_info = message['result'].get('end_device_ids', {})
                            if device_info.get('device_id') == device_id:
                                sensor_data = process_sensor_data(message['result'])
                                if sensor_data:
                                    processed_count += 1
                                    print(f"✅ Processed historical message {processed_count}")
                            else:
                                # Skip messages from other devices
                                continue
                        else:
                            print(f"⚠️ Line {i+1}: No 'result' field found")
                            
                    except json.JSONDecodeError as e:
                        error_count += 1
                        print(f"❌ JSON decode error on line {i+1}: {e}")
                        print(f"   Line content: {line[:100]}...")
                    except Exception as e:
                        error_count += 1
                        print(f"❌ Error processing line {i+1}: {e}")
            
            print(f"🎉 Successfully processed {processed_count} historical messages")
            if error_count > 0:
                print(f"⚠️  {error_count} messages had errors")
            
        else:
            print(f"❌ Error fetching historical data: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error in get_historical_sensor_data: {e}")
        import traceback
        traceback.print_exc()

# Alternative simpler version if the above doesn't work
def get_historical_sensor_data_simple():
    """Simplified version that skips complex parsing"""
    app_id = "bd-test-app2"
    api_key = "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA"
    url = f"https://{ttn_broker}/api/v3/as/applications/{app_id}/packages/storage/uplink_message"

    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"last": "1h"}  # Shorter time for testing
    
    try:
        print("🔄 Fetching historical data (simple method)...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            # Just save the raw data without processing
            with open("historical_data_ndjson.txt", "w", encoding="utf-8") as f:
                f.write(response.text)
            
            print("💾 Historical data saved to historical_data_ndjson.txt")
            print("📊 You can manually inspect this file to see the data format")
            
            # Count non-empty lines
            lines = [line for line in response.text.split('\n') if line.strip()]
            print(f"📨 Found {len(lines)} data entries")
            
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

# MQTT callbacks for TTN
def on_connect_ttn(client, userdata, flags, rc):
    """Callback when connected to TTN broker"""
    if rc == 0:
        print("Connected to TTN MQTT broker!")
        topic = f"v3/{ttn_username}/devices/{device_id}/up"
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect to TTN, return code {rc}")
        print("Reconnect failed, retrying in 5 minutes...")
        time.sleep(5 * 60)

def on_message_ttn(client, userdata, msg):
    """Callback when message received from TTN"""
    print("Message received from TTN")
    
    try:
        payload = json.loads(msg.payload.decode())
        
        # Save raw message
        with open("message.json", "w") as f:
            json.dump(payload, f, indent=4)
        
        # Process and send to AWS
        sensor_data = process_sensor_data(payload)
        
        if sensor_data:
            print("Sensor data successfully processed and sent to AWS")
            print(f"Temperature: {sensor_data.get('temperature')}°C")
            print(f"Humidity: {sensor_data.get('humidity')}%")
            print(f"Battery: {sensor_data.get('battery_voltage')}V")
            print(f"Motion Counts: {sensor_data.get('motion_counts')}")
        else:
            print("Failed to process sensor data")
            
    except Exception as e:
        print(f"Error processing TTN message: {e}")

def setup_ttn_mqtt_client():
    """Set up and return TTN MQTT client"""
    client = mqtt.Client()
    client.username_pw_set(ttn_username, ttn_password)
    client.on_connect = on_connect_ttn
    client.on_message = on_message_ttn
    return client

def main():
    """Main function"""
    print("Starting LHT65N Sensor to AWS Integration...")
    print(f"Monitoring sensor: {device_id}")
    
    # Fetch and process historical data - using the fixed version
    print("Fetching historical data...")
    get_historical_sensor_data()
    
    # If you still have issues, uncomment the line below to use the simple version:
    # get_historical_sensor_data_simple()
    
    # Set up TTN MQTT client for real-time data
    print("Setting up real-time MQTT connection to TTN...")
    ttn_client = setup_ttn_mqtt_client()
    
    try:
        # Connect to TTN and start listening
        ttn_client.connect(ttn_broker, ttn_port, 60)
        print("Starting MQTT loop...")
        ttn_client.loop_forever()
        
    except KeyboardInterrupt:
        print("Stopping application...")
        ttn_client.disconnect()
    except Exception as e:
        print(f"Application error: {e}")
        print("Retrying in 30 seconds...")
        time.sleep(30)
        main()  # Restart

if __name__ == "__main__":
    main()