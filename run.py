#!/usr/bin/env python -tt

"""Reads messages off a queue and saves them in an events schema."""

import pika
import traceback
import sys
import os
import argparse
from collections import namedtuple
from contextlib import closing
from ConfigParser import ConfigParser

from events_consumer import Message, db_connect, process_message

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

QueueConfig = namedtuple('QueueConfig', 'user password host port virtual_host queue')

def queue_config(env):
    filename = '%s_queue.txt'%env
    config = ConfigParser()
    config.read(filename)
    values = config.defaults()
    return QueueConfig(
        values['user'],
        values['password'],
        values['host'],
        int(values['port']),
        values['virtual_host'],
        values['queue'],
    )

def main():
    global db

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('env', help='environment (e.g. development)', nargs='?', default=None)
    args = parser.parse_args()

    env = args.env or os.getenv('aker_events_consumer_env', 'development')
    qc = queue_config(env)
    db = db_connect(env)

    try:
        credentials = pika.PlainCredentials(qc.user, qc.password)
        parameters = pika.ConnectionParameters(qc.host, qc.port, qc.virtual_host, credentials)

        with closing(pika.BlockingConnection(parameters)) as connection:
            channel = connection.channel()
            channel.basic_consume(on_message, qc.queue)
            try:
                print "Listening on %s ..."%qc.queue
                channel.start_consuming()
            finally:
                channel.stop_consuming()
    finally:
        db.close()

if __name__=='__main__':
    main()
