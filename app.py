import os
import random
import sqlite3
import sys
import uuid
import json
from flask_socketio import SocketIO, emit

from flask import (Flask, redirect, render_template_string, request,
                   send_from_directory)

from confluent_kafka import Producer

me = "mohamed-elawadi-1"

conf = { 'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094', 'client.id': me}

producer = Producer (conf)

topic = "abdulrahmankhaled"


def produce_msg(id, filepath):

   msg = {"id": id, "filepath": filepath}  
   producer.produce(topic, key="key", value=json.dumps(msg))

   producer.flush()

   print(f'producer one message, {msg}')


IMAGES_DIR = "images"
MAIN_DB = "main.db"

app = Flask(__name__)
socketio = SocketIO(app)


def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn


con = get_db_connection()
con.execute("CREATE TABLE IF NOT EXISTS image(id, filename, object)")
if not os.path.exists(IMAGES_DIR):
   os.mkdir(IMAGES_DIR)


@app.route('/', methods=['GET'])
def index():
   con = get_db_connection()
   cur = con.cursor()
   res = cur.execute("SELECT * FROM image")
   images = res.fetchall()
   con.close()
   return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.4.0/socket.io.min.js"></script>

<style>
body {
  font-family: Arial, sans-serif;
  margin: 0;
  padding: 20px;
  background-color: #f9f9f9;
}

form {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-bottom: 20px;
}

form div {
  margin: 10px 0;
}

label {
  display: inline-block;
  margin-bottom: 5px;
  font-weight: bold;
}

input[type="file"] {
  display: none;
}

.custom-file-upload {
  border: 1px solid #ccc;
  display: inline-block;
  padding: 6px 12px;
  cursor: pointer;
  background-color: #fff;
  border-radius: 4px;
}

button {
  background-color: #007BFF;
  color: white;
  padding: 10px 20px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

button:hover {
  background-color: #0056b3;
}

.container {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  grid-auto-rows: minmax(100px, auto);
  gap: 20px;
  padding: 10px;
  background-color: #fff;
  border-radius: 8px;
  box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
}

.img {
  height: 270px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.img img {
  max-width: 100%;
  max-height: 100%;
  object-fit: contain;
  border-radius: 8px;
}

.label {
  height: 30px;
  text-align: center;
  font-weight: bold;
  margin-top: 10px;
}

#file-name {
  font-style: italic;
  color: #555;
  margin-top: 10px;
}

</style>
<link rel="shortcut icon" href="#">
<meta charset="UTF-8">

</head>
<body>
<form method="post" enctype="multipart/form-data">
  <div>
    <label class="custom-file-upload">
      <input type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" />
      Choose file to upload
    </label>
  </div>
  <div id="file-name">No file chosen</div>
  <div>
    <button type="submit">Submit</button>
  </div>
</form>
<div class="container">
{% for image in images %}
<div>
  <div class="img">
    <img src="/images/{{ image.filename }}" alt="Uploaded Image">
  </div>
  <div class="label">{{ image.object | default('undefined', true) }}</div>
</div>
{% endfor %}
</div>
<script>
  var fileInput = document.getElementById('file');
  var fileNameDisplay = document.getElementById('file-name');

  fileInput.addEventListener('change', function() {
    var fileName = this.files[0] ? this.files[0].name : 'No file chosen';
    fileNameDisplay.textContent = fileName;
  });

  var socket = io();

  socket.on('connect', function() {
    console.log('Connected to WebSocket');
  });

  socket.on('refresh', function() {
    location.reload();
    console.log('Refreshing.......');
  });
</script>
</body>
</html>
""", images=images)



@app.route('/images/<path:path>', methods=['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)


@app.route('/object/<id>', methods=['PUT'])
def set_object(id):
   con = get_db_connection()
   cur = con.cursor()
   json = request.json
   object = json['object']
   cur.execute("UPDATE image SET object = ? WHERE id = ?", (object, id))
   con.commit()
   con.close()

   return '{"status": "OK"}'


@app.route('/', methods=['POST'])
def upload_file():
   f = request.files['file']
   ext = f.filename.split('.')[-1]
   id = uuid.uuid4().hex
   filename = "{}.{}".format(id, ext)
   f.save("{}/{}".format(IMAGES_DIR, filename))
   con = get_db_connection()
   cur = con.cursor()
   cur.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", (id, filename, ""))
   con.commit()
   con.close()
   produce_msg(id, filename)
   return redirect('/')


current_ws = None


@app.route('/update', methods=['POST'])
def update():
    socketio.emit('refresh', {})
    return "Update Triggered", 200


@socketio.on('connect')
def handle_connect():
    print('Client connected')

		
if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
