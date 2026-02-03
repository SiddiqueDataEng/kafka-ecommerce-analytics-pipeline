#!/usr/bin/env python3
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import json
from datetime import datetime

app = Flask(__name__)
app.config['SECRET_KEY'] = 'test-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Dashboard</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    </head>
    <body>
        <h1>WebSocket Test</h1>
        <div id="status">Connecting...</div>
        <div id="messages"></div>
        
        <script>
            const socket = io();
            
            socket.on('connect', function() {
                document.getElementById('status').textContent = 'Connected!';
                console.log('Connected to WebSocket');
            });
            
            socket.on('disconnect', function() {
                document.getElementById('status').textContent = 'Disconnected';
                console.log('Disconnected from WebSocket');
            });
            
            socket.on('test_event', function(data) {
                const messages = document.getElementById('messages');
                messages.innerHTML += '<p>Received: ' + JSON.stringify(data) + '</p>';
                console.log('Received test event:', data);
            });
        </script>
    </body>
    </html>
    '''

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('test_event', {'message': 'Hello from server', 'timestamp': datetime.now().isoformat()})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    print("Starting minimal test dashboard on http://localhost:5005")
    socketio.run(app, debug=True, host='0.0.0.0', port=5005)