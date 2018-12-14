#!/usr/bin/env python
import os
import pika
import time

pending_acks = []
ch = None

def register_msg(ch, body, delivery_tag, routing_key):
    print("Registering [x] Received {}. Delivery tag: {}. Routing key: {}".format(body, delivery_tag, routing_key))
    pending_acks.append((body, delivery_tag, routing_key))

def finish():
    if len(pending_acks) > 0:
        for pending_ack in pending_acks:
            ch.basic_ack(delivery_tag=pending_ack[1])

    connection.ioloop.stop()
    connection.close()

def on_connection_closed(connection, reply_code, reply_text):
    connection.channel = None
    if connection.is_closing():
        connection.ioloop.stop()

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

    channel.basic_get(callback, queue='gdpr_10001', no_ack=False)
    channel.basic_get(callback, queue='gdpr_10090', no_ack=False)

    # channel.basic_consume(callback, queue='gdpr_10001')
    # channel.basic_consume(callback, queue='gdpr_10090')

if __name__ == '__main__':
    connection = pika.SelectConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST', '127.0.0.1'), port=os.getenv('RABBITMQ_PORT', 5672),
        virtual_host=os.getenv('RABBITMQ_VHOST', '/'),
        credentials=pika.credentials.PlainCredentials(
            username=os.getenv('RABBITMQ_LOGIN', 'guest'),
            password=os.getenv('RABBITMQ_PASSWORD', 'guest')
        )
    ), on_open_callback=on_open, stop_ioloop_on_close=True)

    try:
        print(' [*] Waiting for messages. To exit press CTRL+C')

        # t_end = time.time() + 60 * 15
        # while time.time() < t_end:
        #     # connection.process_data_events()
        #     time.sleep(1)

        connection.ioloop.add_timeout(20, finish)
        connection.ioloop.start()

        # connection.process_data_events(time_limit=0)

    except KeyboardInterrupt:
        connection.close()
