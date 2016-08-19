#!/usr/bin/env python
import pika
import sys
import logging
import json


def main():


    logging.basicConfig()

    credentials = pika.PlainCredentials('RabbitMQAdmin','T3NpCYI7lW6x2O84I120dS')

    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host = 'patrmq1', virtual_host='/',
            credentials=credentials))

    channel = connection.channel()
    queue_name = 'bi'
    global count
    count = 0
    #result = channel.queue_declare(queue=queue_name)

    filename = 'say.txt'
    outfile = open(filename, 'wb')

    print ' [*] Waiting for messages. To exit press CTRL+C'

    def callback(ch, method, properties, body):
        global count
        #print "--------------------------"
        print(body)
        outfile.write(body)


    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

# Start program
if __name__ == "__main__":
   main()