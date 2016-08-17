#!/usr/bin/env python
import pika
import sys
import logging
import json
from py2neo import Graph

def main():
    global graph

    graph = Graph()
    print(graph.uri)

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
        #body = body + '\r\n'
        #print(body)
        count = count + 1
        json_data = json.loads(body)
        if json_data['type'] == 'events.quote.QuoteCreated':
            print(json_data['event']['quote']['address']['city'])
            write_node(json_data)
        #outfile.write(body)
        if count > 1000:
            exit()

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

def write_node(json_data):
    id = json_data['event']['quote']['id']
    lat = json_data['event']['quote']['address']['latitude']
    lng = json_data['event']['quote']['address']['longitude']
    applicant = json_data['event']['quote']['applicant']['lastName']
    quote_node = graph.merge_one("Quote", "id", id)
    quote_node['lat'] = lat
    quote_node['lng'] = lng
    quote_node['applicant'] = applicant

    quote_node.push()

# Start program
if __name__ == "__main__":
   main()