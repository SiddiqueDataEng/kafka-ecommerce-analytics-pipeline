#!/usr/bin/env python3
import socketio
import time

# Create a Socket.IO client
sio = socketio.Client()

@sio.event
def connect():
    print('✅ Connected to WebSocket server')

@sio.event
def disconnect():
    print('❌ Disconnected from WebSocket server')

@sio.on('kafka_raw_event')
def on_kafka_raw_event(data):
    print(f'📡 Received Kafka raw event: {data.get("type", "unknown")}')
    if 'data' in data and 'value' in data['data']:
        event = data['data']['value']
        print(f'   Event: {event.get("event_type", "unknown")} | Customer: {event.get("customer_id", "unknown")}')

@sio.on('new_event')
def on_new_event(data):
    print(f'🆕 Received new event: {data.get("type", "unknown")}')

def test_websocket():
    try:
        print("Connecting to WebSocket server...")
        sio.connect('http://localhost:5004')
        
        print("Listening for events for 30 seconds...")
        time.sleep(30)
        
        sio.disconnect()
        print("Test completed")
        
    except Exception as e:
        print(f"❌ WebSocket test failed: {e}")

if __name__ == "__main__":
    test_websocket()