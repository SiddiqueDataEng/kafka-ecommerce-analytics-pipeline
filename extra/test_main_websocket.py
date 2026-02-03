#!/usr/bin/env python3
import socketio
import time

# Create a Socket.IO client
sio = socketio.Client()

@sio.event
def connect():
    print('✅ Connected to main WebSocket server')

@sio.event
def disconnect():
    print('❌ Disconnected from main WebSocket server')

@sio.on('kafka_raw_event')
def on_kafka_raw_event(data):
    print(f'📡 Received Kafka raw event!')
    print(f'   Type: {data.get("type", "unknown")}')
    if 'data' in data and 'value' in data['data']:
        event = data['data']['value']
        print(f'   Event: {event.get("event_type", "unknown")} | Customer: {event.get("customer_id", "unknown")}')

def test_main_websocket():
    try:
        print("Connecting to main WebSocket server...")
        sio.connect('http://localhost:5004', wait_timeout=10)
        
        print("Connected! Listening for events for 15 seconds...")
        time.sleep(15)
        
        sio.disconnect()
        print("Main WebSocket test completed")
        
    except Exception as e:
        print(f"❌ Main WebSocket test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_main_websocket()