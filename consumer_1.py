from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import random
import requests
import json
from PIL import Image, ImageFont, ImageDraw
import matplotlib.pyplot as plt
import numpy as np
import time


IMAGES_DIR = "images"


def msg_process(msg):
    json_msg = json.loads(msg.value().decode())
    filepath = json_msg['filepath']
    label = json_msg['label']
    image = Image.open(f"{filepath}")
    w, h = image.size
    x, y = int(w / 2), int(h / 2)
    if x > y:
        font_size = y
    else:
        font_size = x
    font = ImageFont.load_default(int(font_size / 3))
    draw = ImageDraw.Draw(image)
    draw.text((x, h / 3), label or "Nothing", font = font, fill=(0, 0, 0), anchor='ms')
    image.save(filepath)
    print(f"Watermark added for a new image with path: {filepath}")


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
                msg_process(msg)
                time.sleep(0.03)
                requests.post('http://127.0.0.1:5000/update')
                consumer.commit(asynchronous=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


me = "mohamed-elawadi-1"

conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094', 'group.id': 'foo2', 'enable.auto.commit': 'True', 'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

basic_consume_loop(consumer, ['abdulrahmankhaled'])

