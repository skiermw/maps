#!/usr/bin/env python
import boto3
import json
from py2neo import Graph, Relationship

def main():
    global graph

    graph = Graph()
    print(graph.uri)

    s3 = boto3.resource('s3')

    bucket = s3.Bucket('say-bi-prod')
    for item in bucket.objects.all():
        if 'BI_Messages_2016-08-19/' in item.key:
            #msg_file = item.get()['Body'].read()
            msg_file = item.get()['Body']
            print(msg_file)
            json_data = json.load(msg_file)
            #json_data = msg_file
            if 'type' in json_data:
                if json_data['type'] == 'events.quote.QuoteCreated':
                    write_quote_node(json_data)
                elif json_data['type'] == 'events.policy.PolicyCreated':
                    write_policy_node(json_data)


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
    lat = json_data['event']['policy']['address']['latitude']
    lng = json_data['event']['policy']['address']['longitude']
    applicant = json_data['event']['policy']['applicant']['lastName']
    policy_node = graph.merge_one("Policy", "id", id)
    policy_node['applicant'] = applicant
    policy_node['lat'] = lat
    policy_node['lng'] = lng
    policy_node.push()

    ip_node = graph.merge_one("IP", "ipAddress", ip)
    results = graph.create_unique(Relationship(policy_node, "CREATED_FROM", ip_node))

    address_node = write_address_node(json_data['event']['policy']['address'])
    results = graph.create_unique(Relationship(policy_node, "LOCATED_AT", address_node))

# Start program
if __name__ == "__main__":
   main()