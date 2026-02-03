#!/usr/bin/env python3
import socketio
import time

# Create a Socket.IO client
sio = socketio.Client()

@sio.event
def connect():
    print('✅ Connected to WebSocket server')
    sio.emit('test_message', {'data': 'Hello from client'})

@sio.event
def disconnect():
    print('❌ Disconnected from WebSocket server')

@sio.on('test_response')
def on_test_response(data):
    print(f'📡 Received test response: {data}')

def test_simple_websocket():
    try:
        print("Connecting to WebSocket server...")
        sio.connect('http://localhost:5004', wait_timeout=10)
        
        print("Connected! Waiting 5 seconds...")
        time.sleep(5)
        
        sio.disconnect()
        print("Test completed")
        
    except Exception as e:
        print(f"❌ WebSocket test failed: {e}")

if __name__ == "__main__":
    test_simple_websocket()