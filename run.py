#!/usr/bin/env python -tt

"""Reads messages off a queue and saves them in an events schema."""

import pika
import traceback
import sys
import os
import argparse
import smtplib
from email.mime.text import MIMEText
from collections import namedtuple
from contextlib import closing
from ConfigParser import ConfigParser
from functools import partial

from events_consumer import Message, db_connect, process_message

def on_message(channel, method_frame, header_frame, body, **kwargs):
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
        try:
            # Nack the message without requeueing. Message will be resent to DLX
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

            print
            print 'Error processing message. Not acknowledging.'

            # Notify everyone that processing failed
            notify_process_fail(body, **kwargs)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            print 'Failed to nack message.'

            # Notify everyone that something went wrong while nacking a message
            notify_nack_fail(body, **kwargs)

def notify_process_fail(message_body, **kwargs):
    reason = 'The following message failed to be processed in the Aker Events Consumer'
    notify(reason, message_body, **kwargs)

def notify_nack_fail(message_body, **kwargs):
    reason = 'The following message failed to be processed and could not be nacked'
    notify(reason, message_body, **kwargs)

def notify(reason, message_body, env, email_config):
    if env not in ['staging', 'production',]:
        pass

    text = '''
    %s:

    %s

    %s

    Yours sincerely,
    Aker Events Consumer
    ''' % (reason, message_body, traceback.format_exc())

    msg = MIMEText(text)
    msg['Subject'] = 'Aker Events Consumer: Message Processing Failed'
    msg['From'] = email_config.from_address
    msg['To'] = email_config.to

    s = smtplib.SMTP(email_config.smtp_address)
    s.sendmail(email_config.to, email_config.to.split(','), msg.as_string())
    s.quit()

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

EmailConfig = namedtuple('EmailConfig', 'from_address to smtp_address')

def email_config(env):
    filename = '%s_email.txt'%env
    config = ConfigParser()
    config.read(filename)
    values = config.defaults()
    return EmailConfig(
        values['from_address'],
        values['to'],
        values['smtp_address'],
    )

def main():
    global db

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('env', help='environment (e.g. development)', nargs='?', default=None)
    args = parser.parse_args()

    env = args.env or os.getenv('aker_events_consumer_env', 'development')

    if env not in ('development', 'test', 'staging', 'production'):
        raise ValueError("Unrecognised environment: %r"%env)

    qc = queue_config(env)
    db = db_connect(env)
    ec = email_config(env)

    on_message_partial = partial(on_message, env=env, email_config=ec)

    try:
        credentials = pika.PlainCredentials(qc.user, qc.password)
        parameters = pika.ConnectionParameters(qc.host, qc.port, qc.virtual_host, credentials)

        with closing(pika.BlockingConnection(parameters)) as connection:
            channel = connection.channel()
            channel.basic_consume(on_message_partial, qc.queue)
            try:
                print "Listening on %s ..."%qc.queue
                channel.start_consuming()
            finally:
                channel.stop_consuming()
    finally:
        db.close()

if __name__=='__main__':
    main()
