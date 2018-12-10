#!/usr/bin/env python
import os
import pika
import time
import threading

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def create_connection_for_queue(queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST', '127.0.0.1'), port=os.getenv('RABBITMQ_PORT', 5672),
        virtual_host=os.getenv('RABBITMQ_VHOST', '/'),
        credentials=pika.credentials.PlainCredentials(
            username=os.getenv('RABBITMQ_LOGIN', 'guest'),
            password=os.getenv('RABBITMQ_PASSWORD', 'guest')
        )
    ))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=queue, arguments={'queue': queue})
    return channel

def consume(ev, queue):
    print("[*] Creating connection for queue: {}".format(queue))
    channel = create_connection_for_queue(queue)
    print("[*] Created connection for queue: {}".format(queue))
    while not ev.is_set():
        channel.process_data_events(time_limit=0.1)

    channel.close()

if __name__ == '__main__':
    queues = ['datalake_10001', 'datalake_10090']
    workers = 10

    for i in range(0, workers):
        ev = threading.Event()
        thr = threading.Thread(target=consume, args=(ev, ), kwargs={'queue': queues[i % len(queues)]})
        thr.start()
        ev.set()
        thr.join()
        print("[m] Finished")






