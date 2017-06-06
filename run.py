#!/usr/bin/env python -tt

"""Reads messages off a queue and saves them in an events schema."""

import pika
import traceback
import sys
import os
import argparse

from events_consumer import Message, db_connect, process_message
from contextlib import closing

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

def main():
    global db

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('env', help='environment (e.g. development)', nargs='?', default=None)
    args = parser.parse_args()

    env = args.env or os.getenv('aker_events_consumer_env', 'development')
    db = db_connect(env)

    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

    with closing(pika.BlockingConnection(parameters)) as connection:
        channel = connection.channel()
        channel.basic_consume(on_message, 'aker.events')
        try:
            print "Listening ..."
            channel.start_consuming()
        finally:
            channel.stop_consuming()

    db.close()

if __name__=='__main__':
    main()
