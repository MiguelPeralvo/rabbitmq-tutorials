#!/usr/bin/env python
import os
import pika
import time
import multiprocessing

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

def consume(queue):
    print("[*] Creating connection for queue: {}".format(queue))
    channel = create_connection_for_queue(queue)
    print("[*] Created connection for queue: {}".format(queue))
    channel.start_consuming()

if __name__ == '__main__':
    queues = ['datalake_10001', 'datalake_10090']
    workers = 10
    pool = multiprocessing.Pool(processes=workers)

    for i in range(0, workers):
        pool.apply_async(consume, kwds={'queue': queues[i % len(queues)]})

    # Stay alive
    try:
        while True:
            continue
    except KeyboardInterrupt:
        print('[*] Exiting...')
        pool.terminate()
        pool.join()

    print("[*] Waiting for messages. To exit press CTRL+C'")






