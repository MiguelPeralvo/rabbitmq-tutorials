#!/usr/bin/env python
import os
import pika
import time


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_open(connection):
    connection.channel(on_channel_open)


def on_channel_open(channel):
    channel.basic_consume(callback, queue='datalake_10001')
    channel.basic_consume(callback, queue='datalake_10090')

if __name__ == '__main__':
    connection = pika.SelectConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST', '127.0.0.1'), port=os.getenv('RABBITMQ_PORT', 5672),
        virtual_host=os.getenv('RABBITMQ_VHOST', '/'),
        credentials=pika.credentials.PlainCredentials(
            username=os.getenv('RABBITMQ_LOGIN', 'guest'),
            password=os.getenv('RABBITMQ_PASSWORD', 'guest')
        )
    ), on_open_callback=on_open)

    try:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()