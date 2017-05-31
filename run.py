#!/usr/bin/env python -tt

import pika
import traceback
import sys

from events_consumer import Message

def on_message(channel, method_frame, header_frame, body):
    try:
        print method_frame.delivery_tag
        print body
        message = Message.from_json(body)
        print message
        print
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    except Exception as e:
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write('\n')

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.basic_consume(on_message, 'aker.events')

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

connection.close()
