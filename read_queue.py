#!/usr/bin/env python
import pika
import sys
import logging
import json
import write_nodes
from py2neo import Graph, Relationship, authenticate

def main():
    global graph

    #graph = Graph()
    authenticate("http://10.8.30.145:7474/", "neo4j", "shelter")
    graph = Graph("http://neo4j:shelter@10.8.30.145:7474/db/data/")
    print(graph.uri)
    logging.basicConfig()

    credentials = pika.PlainCredentials('RabbitMQAdmin','T3NpCYI7lW6x2O84I120dS')

    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host = 'paprmq1', virtual_host='/',
            credentials=credentials))

    channel = connection.channel()
    queue_name = 'test_q'
    global count
    count = 0

    print ' [*] Waiting for messages. To exit press CTRL+C'

    def callback(ch, method, properties, body):
        #print(body)

        json_data = json.loads(body)

        if 'type' in json_data:
            if json_data['type'] == 'events.quote.QuoteCreated':
                write_nodes.write_quote_node(graph, json_data)
            elif json_data['type'] == 'events.policy.PolicyCreated':
                write_nodes.write_policy_number_node(graph, json_data)
            elif json_data['type'] == 'events.policy.PolicyOverwritten':
                    print('Policy overwritten')
                    write_nodes.overwrite_policy(graph, json_data)
            else:
                if 'events.policy' in json_data['type']:
                    print('Found Policy Type %s' % json_data['type'])

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

# Start program
if __name__ == "__main__":
   main()