#!/usr/bin/env python
import boto3
import json
from py2neo import Graph, Relationship, authenticate

def main():
    global graph
    authenticate("localhost:7474", "neo4j", "hyenas")
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
    applicant = json_data['event']['quote']['applicant']['lastName'] +', ' + json_data['event']['quote']['applicant']['firstName']
    quote_node = graph.merge_one("Quote", "id", id)
    quote_node['applicant'] = applicant
    quote_node['lat'] = lat
    quote_node['lng'] = lng
    quote_node.push()
    address_node = write_address_node(json_data['event']['quote']['address'])
    results = graph.create_unique(Relationship(quote_node, "LOCATED_AT", address_node))

    applicant_node = write_person_node(json_data['event']['quote']['applicant'])
    results = graph.create_unique(Relationship(applicant_node, "APPLICANT_OF", quote_node))

def write_address_node(address):
    print('   Writing Address')
    lat = address['latitude']
    lng = address['longitude']
    street = address['street']
    city = address['city']
    county = address['county']
    state = address['state']
    zip = address['zip']
    countyFIPS = address['countyFIPS']

    address_key = street + zip[:5]
    address_node = graph.merge_one("Address", "address_key", address_key)
    address_node['lat'] = lat
    address_node['lng'] = lng
    address_node['street'] = street
    address_node['city'] = city
    address_node['county'] = county
    address_node['state'] = state
    address_node['zip'] = zip
    address_node['countyFIPS'] = countyFIPS
    address_node.push()

    return address_node


def write_policy_node(json_data):
    print('Writing Policy')
    id = json_data['event']['policy']['id']
    ip = json_data['event']['ipAddress']
    lat = json_data['event']['policy']['address']['latitude']
    lng = json_data['event']['policy']['address']['longitude']
    applicant = json_data['event']['policy']['applicant']['lastName'] +', ' + json_data['event']['policy']['applicant']['firstName']
    policy_node = graph.merge_one("Policy", "policyNumber", json_data['event']['policy']['policyNumber'])
    policy_node['applicant'] = applicant

    policy_node['lat'] = lat
    policy_node['lng'] = lng

    policy_node['effectiveDate'] = json_data['event']['policy']['effectiveDate']
    policy_node['expiratioinDate'] = json_data['event']['policy']['expirationDate']
    policy_node['familyNumber'] = json_data['event']['policy']['familyNumber']
    policy_node['paymentPlan'] = json_data['event']['policy']['paymentPlan']
    policy_node['totalPremium'] = json_data['event']['policy']['totalPremium']
    policy_node['id'] = json_data['event']['policy']['id']
    policy_node['channelOfOrigin'] = json_data['event']['policy']['channelOfOrigin']

    policy_node.push()

    for driver in json_data['event']['policy']['drivers']:
        driver_node = write_person_node(driver)
        results = graph.create_unique(Relationship(driver_node, "DRIVER_OF", policy_node))

    for vehicle in json_data['event']['policy']['vehicles']:
        vehicle_node = write_vehicle_node(vehicle)
        results = graph.create_unique(Relationship(vehicle_node, "COVERED_BY", policy_node))

    ip_node = graph.merge_one("IP", "ipAddress", ip)
    results = graph.create_unique(Relationship(policy_node, "CREATED_FROM", ip_node))

    address_node = write_address_node(json_data['event']['policy']['address'])
    results = graph.create_unique(Relationship(policy_node, "LOCATED_AT", address_node))

    applicant_node = write_person_node(json_data['event']['policy']['applicant'])
    results = graph.create_unique(Relationship(applicant_node, "APPLICANT_OF", policy_node))

def write_vehicle_node(vehicle_data):
    print('Writing Vehicle')

    vehicle_node = graph.merge_one("Vehicle", "vin", vehicle_data['vin'])
    vehicle_node['make'] = vehicle_data['make']
    vehicle_node['model'] = vehicle_data['model']
    vehicle_node['year'] = vehicle_data['year']
    vehicle_node['ownership'] = vehicle_data['ownership']

    vehicle_node.push()

    return vehicle_node

def write_person_node(person_data):
    print('Writing Person')

    clientId = person_data['clientId']
    person_node = graph.merge_one("Person", "clientId", clientId)
    person_node['age'] = person_data['age']
    person_node['birthDate'] = person_data['birthDate']
    person_node['firstName'] = person_data['firstName']
    person_node['lastName'] = person_data['lastName']
    person_node.push()

    return person_node


# Start program
if __name__ == "__main__":
   main()