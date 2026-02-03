#!/usr/bin/env python3
import socketio
import time

# Create a Socket.IO client
sio = socketio.Client()

@sio.event
def connect():
    print('✅ Connected to minimal WebSocket server')

@sio.event
def disconnect():
    print('❌ Disconnected from minimal WebSocket server')

@sio.on('test_event')
def on_test_event(data):
    print(f'📡 Received test event: {data}')

def test_minimal_websocket():
    try:
        print("Connecting to minimal WebSocket server...")
        sio.connect('http://localhost:5005', wait_timeout=10)
        
        print("Connected! Waiting 5 seconds...")
        time.sleep(5)
        
        sio.disconnect()
        print("Minimal WebSocket test completed successfully")
        
    except Exception as e:
        print(f"❌ Minimal WebSocket test failed: {e}")

if __name__ == "__main__":
    test_minimal_websocket()