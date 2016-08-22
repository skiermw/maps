#!/usr/bin/env python
import pika
import sys
import logging
import json
import write_nodes
from py2neo import Graph, Relationship

def main():
    global graph

    graph = Graph()
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
        global count
        #print "--------------------------"
        #body = body + '\r\n'
        print(body)

        json_data = json.loads(body)

        if 'type' in json_data:
            if json_data['type'] == 'events.quote.QuoteCreated':
                write_nodes.write_quote_node(graph, json_data)
            elif json_data['type'] == 'events.policy.PolicyCreated':
                write_nodes.write_policy_node(graph, json_data)
        #outfile.write(body)
        #if count > 1000:
        #    exit()

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()
'''
def write_quote_node(json_data):
    print('Writing Quote')
    id = json_data['event']['quote']['id']
    lat = json_data['event']['quote']['address']['latitude']
    lng = json_data['event']['quote']['address']['longitude']
    applicant = json_data['event']['quote']['applicant']['lastName']
    quote_node = graph.merge_one("Quote", "id", id)
    quote_node['applicant'] = applicant
    quote_node['lat'] = lat
    quote_node['lng'] = lng
    quote_node.push()
    address_node = write_address_node(json_data['event']['quote']['address'])
    results = graph.create_unique(Relationship(quote_node, "LOCATED_AT", address_node))

def write_address_node(address):
    print('   Writing Address')
    lat = address['latitude']
    lng = address['longitude']
    street = address['street']
    zip = address['zip']
    address_key = street + zip[:5]
    address_node = graph.merge_one("Address", "address_key", address_key)
    address_node['lat'] = lat
    address_node['lng'] = lng
    address_node['street'] = street
    address_node['zip'] = zip
    address_node.push()

    return address_node


def write_policy_node(json_data):
    print('Writing Policy')
    id = json_data['event']['policy']['id']
    ip = json_data['event']['ipAddress']
    applicant = json_data['event']['policy']['applicant']['lastName']
    policy_node = graph.merge_one("Policy", "id", id)
    policy_node['applicant'] = applicant
    policy_node.push()

    ip_node = graph.merge_one("IP", "ipAddress", ip)
    results = graph.create_unique(Relationship(policy_node, "CREATED_FROM", ip_node))

    address_node = write_address_node(json_data['event']['policy']['address'])
    results = graph.create_unique(Relationship(policy_node, "LOCATED_AT", address_node))
'''
# Start program
if __name__ == "__main__":
   main()