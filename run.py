#!/usr/bin/env python -tt

import pika
import traceback
import sys
import os

from events_consumer import Message, setup_database, process_message

def on_message(channel, method_frame, header_frame, body):
    try:
        print method_frame.routing_key
        print method_frame.delivery_tag
        print body
        message = Message.from_json(body)
        print message
        process_message(db, message)
        print
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    except Exception:
        traceback.print_exc(file=sys.stderr)

env = os.getenv('aker_events_consumer_env', 'development')

db = setup_database(env)

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.basic_consume(on_message, 'aker.events')

try:
    channel.start_consuming()
finally:
    channel.stop_consuming()
    connection.close()
    db.close()
