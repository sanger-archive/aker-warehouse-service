#!/usr/bin/env python -tt

"""Reads messages off a queue and saves them in an events schema."""

import pika
import traceback
import sys
import os
import argparse
import smtplib
from daemon import DaemonContext, pidfile
from email.mime.text import MIMEText
from contextlib import closing
from functools import partial

from events_consumer import Message, db_connect, process_message, Config

def on_message(channel, method_frame, header_frame, body, db, env, config):
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
            notify_process_fail(body, env, config)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            print 'Failed to nack message.'

            # Notify everyone that something went wrong while nacking a message
            notify_nack_fail(body, env, config)

def notify_process_fail(message_body, env, config):
    reason = 'The following message failed to be processed in the Aker Events Consumer'
    notify(reason, message_body, env, config)

def notify_nack_fail(message_body, env, config):
    reason = 'The following message failed to be processed and could not be nacked'
    notify(reason, message_body, env, config)

def notify(reason, message_body, env, config):
    if env not in ['staging', 'production']:
        return

    signoff = 'Yours sincerely,\nAkerEventsConsumer'

    text = '\n{}:\n\n{}\n\n{}\n\n{}\n'.format(reason, message_body, traceback.format_exc(), signoff)

    msg = MIMEText(text)
    msg['Subject'] = 'Aker Events Consumer: Message Processing Failed'
    msg['From'] = config.email.from_address
    msg['To'] = config.email.to

    s = smtplib.SMTP(config.email.smtp_address)
    s.sendmail(config.email.to, config.email.to.split(','), msg.as_string())
    s.quit()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('env', help='environment (e.g. development)', nargs='?', default=None)
    args = parser.parse_args()

    env = args.env or os.getenv('aker_events_consumer_env', 'development')

    if env not in ('development', 'test', 'staging', 'production'):
        raise ValueError("Unrecognised environment: %r"%env)

    config = Config('%s/%s.txt'%(os.path.dirname(os.path.realpath(__file__)), env))

    # See https://pagure.io/python-daemon/blob/master/f/daemon/daemon.py#_63 for docs
    with DaemonContext(
            working_directory=os.getcwd(),
            stdout=open(config.process.logfile, 'w'),
            stderr=open(config.process.errorlog, 'w'),
            pidfile=pidfile.PIDLockFile(config.process.pidfile),
        ):
        db = db_connect(config)

        on_message_partial = partial(on_message, db=db, env=env, config=config)

        try:
            credentials = pika.PlainCredentials(config.message_queue.user, config.message_queue.password)
            parameters = pika.ConnectionParameters(config.message_queue.host, config.message_queue.port, config.message_queue.virtual_host, credentials)

            with closing(pika.BlockingConnection(parameters)) as connection:
                channel = connection.channel()
                channel.basic_consume(on_message_partial, config.message_queue.queue)
                try:
                    print "Listening on %s ..."%config.message_queue.queue
                    channel.start_consuming()
                finally:
                    channel.stop_consuming()
        finally:
            db.close()

if __name__ == '__main__':
    main()
