from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import random
import requests
import json
from PIL import Image, ImageFont, ImageDraw
import matplotlib.pyplot as plt
import numpy as np
import cv2

IMAGES_DIR = "images"


def msg_process(msg):
    json_msg = json.loads(msg.value().decode())
    filepath = json_msg['filepath']
    id = json_msg['id']
    image_path = f"{IMAGES_DIR}/{filepath}"
    label = detect_obj(image_path)
    requests.put('http://127.0.0.1:5000/object/' + id, json={"object":label })

    print(f"detect for a new image with path: {filepath}")
    return id, image_path, label


def detect_obj(image_path):

    original_image = cv2.imread(image_path)

    # Convert the image to grayscale for easier computation
    image_grey = cv2.cvtColor(original_image, cv2.COLOR_RGB2GRAY)

    face_classifier = cv2.CascadeClassifier(
        f"{cv2.data.haarcascades}haarcascade_frontalface_alt.xml")
    
    cat_classifier = cv2.CascadeClassifier(
        f"{cv2.data.haarcascades}haarcascade_frontalcatface_extended.xml")
    
    plate_classifier = cv2.CascadeClassifier(
        f"{cv2.data.haarcascades}haarcascade_russian_plate_number.xml")

    detected_face = face_classifier.detectMultiScale(image_grey, minSize=(50, 50))
    detected_cats = cat_classifier.detectMultiScale(image_grey, minSize=(50, 50))
    detected_plates = plate_classifier.detectMultiScale(image_grey, minSize=(50, 50))

    label = "Nothing"
    if len(detected_face) != 0:
        label = "Person"
        print("detected person")
        for (x, y, width, height) in detected_face:
            cv2.rectangle(original_image, (x, y),
                        (x + height, y + width),
                        (255, 0, 0), 2)
            
    if len(detected_cats) != 0:
        label = "Cat"
        print("detected cat")
        for (x, y, width, height) in detected_cats:
            cv2.rectangle(original_image, (x, y),
                        (x + height, y + width),
                        (255, 0, 0), 2)
    
    if len(detected_plates) != 0:
        label = "Plate Number"
        print("detected plate")
        for (x, y, width, height) in detected_plates:
            cv2.rectangle(original_image, (x, y),
                        (x + height, y + width),
                        (255, 0, 0), 2)
            
    cv2.imwrite(image_path, original_image)

    return label


me = "mohamed-elawadi-1"

conf = { 'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094', 'client.id': me}

producer = Producer (conf)

topic = "abdulrahmankhaled"


def produce_msg(id, filepath, label):

   msg = {"id": id, "filepath": filepath, "label": label}  
   producer.produce(topic, key="key", value=json.dumps(msg))

   producer.flush()

   print(f'producer one message')


running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' % 
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                id, path, label = msg_process(msg)
                produce_msg(id, path, label)
                consumer.commit(asynchronous=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


conf2 = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094', 'group.id': 'foo22', 'enable.auto.commit': 'True', 'auto.offset.reset': 'earliest'}

consumer = Consumer(conf2)

basic_consume_loop(consumer, ['abdulrahmankhaled'])
