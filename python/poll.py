#!/usr/bin/env python
import os
import pika
import time
import logging

pending_acks = []
ch = None

def register_msg(ch, body, delivery_tag, routing_key):
    print("Registering [x] Received {}. Delivery tag: {}. Routing key: {}".format(body, delivery_tag, routing_key))
    pending_acks.append((body, delivery_tag, routing_key))
    ch.basic_ack(delivery_tag=delivery_tag)

def finish():

    if len(pending_acks) > 0:
        for pending_ack in pending_acks:
            ch.basic_ack(delivery_tag=pending_ack[1])
            print("Ack-ed: {}".format(pending_ack[1]))

    connection.ioloop.stop()
    connection.close()

def callback(ch, method, properties, body):
    register_msg(ch, body, method.delivery_tag, method.routing_key)
    # print(" [x] Received {}. Delivery tag: {}. Routing key: {}".format(body, method.delivery_tag, method.routing_key))
    # time.sleep(body.count(b'.'))
    print(" [x] Done")
    # ch.basic_ack(delivery_tag=method.delivery_tag)

def on_open(connection):
    connection.channel(on_channel_open)

def on_channel_open(channel):
    global ch
    ch = channel

    channel.basic_consume(callback, queue='gdpr_10001')
    channel.basic_consume(callback, queue='gdpr_10090')


if __name__ == '__main__':
    global connection

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
        connection.ioloop.add_timeout(5, finish)
        connection.ioloop.start()

        # connection.process_data_events(time_limit=0)

    except KeyboardInterrupt:
        connection.close()
