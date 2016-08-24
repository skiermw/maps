from py2neo import Graph, Relationship, authenticate
import requests
import json

def write_quote_node(graph, json_data):
    print('Writing Quote')
    id = json_data['event']['quote']['id']
    applicant = json_data['event']['quote']['applicant']['lastName'] +', ' + json_data['event']['quote']['applicant']['firstName']
    quote_node = graph.merge_one("Quote", "id", id)
    quote_node['applicant'] = applicant
    quote_node['lat'] = json_data['event']['quote']['address']['latitude']
    quote_node['lng'] = json_data['event']['quote']['address']['longitude']
    quote_node.push()
    address_node = write_address_node(graph, json_data['event']['quote']['address'])
    results = graph.create_unique(Relationship(quote_node, "LOCATED_AT", address_node))

    applicant_node = write_person_node(graph, 'QUOTE_APPLICANT', json_data['event']['quote']['applicant'])
    results = graph.create_unique(Relationship(applicant_node, "APPLICANT_OF", quote_node))

def write_address_node(graph, address):
    print('   Writing Address')

    address_key = address['street'] + address['zip'][:5]
    address_node = graph.merge_one("Address", "address_key", address_key)
    address_node['lat'] = address['latitude']
    address_node['lng'] = address['longitude']
    address_node['street'] = address['street']
    address_node['city'] = address['city']
    address_node['county'] = address['county']
    address_node['state'] = address['state']
    address_node['zip'] = address['zip']
    address_node['countyFIPS'] = address['countyFIPS']
    address_node['id'] = address['id']
    address_node.push()

    return address_node


def write_policy_node(graph, json_data):
    print('Writing Policy')

    policy_node = graph.merge_one("Policy", "policyNumber", json_data['event']['policy']['policyNumber'])
    applicant = json_data['event']['policy']['applicant']['lastName'] +', ' + json_data['event']['policy']['applicant']['firstName']
    policy_node['applicant'] = applicant

    policy_node['lat'] = json_data['event']['policy']['address']['latitude']
    policy_node['lng'] = json_data['event']['policy']['address']['longitude']

    policy_node['effectiveDate'] = json_data['event']['policy']['effectiveDate']
    policy_node['expirationDate'] = json_data['event']['policy']['expirationDate']
    policy_node['evidenceOfPriorInsurance'] = json_data['event']['policy']['evidenceOfPriorInsurance']
    policy_node['familyNumber'] = json_data['event']['policy']['familyNumber']
    policy_node['fcraNoticeRequired'] = json_data['event']['policy']['fcraNoticeRequired']
    policy_node['firstPolicyEffectiveDate'] = json_data['event']['policy']['firstPolicyEffectiveDate']
    policy_node['firstPolicySubmissionDate'] = json_data['event']['policy']['firstPolicySubmissionDate']
    policy_node['paymentPlan'] = json_data['event']['policy']['paymentPlan']
    policy_node['renewableStatus'] = json_data['event']['policy']['renewableStatus']
    policy_node['revision'] = json_data['event']['policy']['revision']
    policy_node['revisionTimestamp'] = json_data['event']['policy']['revisionTimestamp']
    policy_node['status'] = json_data['event']['policy']['status']
    policy_node['term'] = json_data['event']['policy']['term']
    policy_node['territory'] = json_data['event']['policy']['territory']
    policy_node['totalDiscount'] = json_data['event']['policy']['totalDiscount']
    policy_node['totalPremium'] = json_data['event']['policy']['totalPremium']
    policy_node['id'] = json_data['event']['policy']['id']
    policy_node['lineOfBusiness'] = json_data['event']['policy']['lineOfBusiness']
    policy_node['channelOfOrigin'] = json_data['event']['policy']['channelOfOrigin']
    policy_node['quoteId'] = json_data['event']['quoteId']
    policy_node['company'] = json_data['event']['policy']['company']
    policy_node['compositeDriverRatingFactorWithPoints'] = json_data['event']['policy']['compositeDriverRatingFactorWithPoints']
    policy_node['compositeDriverRatingFactorWithoutPoints'] = json_data['event']['policy']['compositeDriverRatingFactorWithoutPoints']
    policy_node['documentDeliveryMethod'] = json_data['event']['policy']['documentDeliveryMethod']
    policy_node.push()

    for discount in json_data['event']['policy']['discounts']:
        discount_node = graph.merge_one("Discount", "type", discount['type'])
        results = graph.create_unique(Relationship( policy_node, "HAS_DISCOUNT", discount_node))

    for driver in json_data['event']['policy']['drivers']:
        driver_node = write_person_node(graph, 'DRIVER', driver)
        results = graph.create_unique(Relationship(driver_node, "DRIVER_OF", policy_node))

    for namedInsured in json_data['event']['policy']['namedInsureds']:
        namedInsured_node = write_person_node(graph, 'NAMED_INSURED', namedInsured)
        results = graph.create_unique(Relationship(namedInsured_node, "NAMEDINSURED_OF", policy_node))

    for vehicle in json_data['event']['policy']['vehicles']:
        vehicle_node = write_vehicle_node(graph, vehicle)
        results = graph.create_unique(Relationship(vehicle_node, "COVERED_BY", policy_node))


    ip_node = write_ip_node(graph, json_data['event']['ipAddress'])
    results = graph.create_unique(Relationship(policy_node, "CREATED_FROM", ip_node))

    address_node = write_address_node(graph, json_data['event']['policy']['address'])
    results = graph.create_unique(Relationship(policy_node, "LOCATED_AT", address_node))

    applicant_node = write_person_node(graph, 'APPLICANT', json_data['event']['policy']['applicant'])
    results = graph.create_unique(Relationship(applicant_node, "APPLICANT_OF", policy_node))

def write_ip_node(graph, ip):
    print('   Writing IP')
    ip_node = graph.merge_one("IP", "ipAddress", ip)
    url = 'http://ipinfo.io/%s' % ip
    response = requests.get(url)
    if response.status_code != 200:
        print('IP status: %s' % response.status_code)
    else:
        ip_json = json.loads(response.text)
        if 'hostname' in ip_json:
            ip_node['hostname'] = ip_json['hostname']
        if 'city' in ip_json:
            ip_node['city'] = ip_json['city']
        if 'state' in ip_json:
            ip_node['state'] = ip_json['region']
        if 'country' in ip_json:
            ip_node['country'] = ip_json['country']
        if 'loc' in ip_json:
            loc_list = ip_json['loc'].split(',')
            ip_node['lat'] = float(loc_list[0])
            ip_node['lng'] = float(loc_list[1])

    ip_node.push()

    return ip_node

def write_vehicle_node(graph, vehicle_data):
    print('   Writing Vehicle')

    vehicle_node = graph.merge_one("Vehicle", "vin", vehicle_data['vin'])
    vehicle_node['antiTheftDevice'] = vehicle_data['antiTheftDevice']
    vehicle_node['businessUse'] = vehicle_data['businessUse']
    vehicle_node['costSymbol'] = vehicle_data['costSymbol']
    vehicle_node['id'] = vehicle_data['id']
    vehicle_node['make'] = vehicle_data['make']
    vehicle_node['model'] = vehicle_data['model']
    vehicle_node['ownership'] = vehicle_data['ownership']
    vehicle_node['pseudoVin'] = vehicle_data['pseudoVin']
    vehicle_node['sequence'] = vehicle_data['trim']
    vehicle_node['trim'] = vehicle_data['trim']
    vehicle_node['trimCode'] = vehicle_data['trimCode']
    vehicle_node['year'] = vehicle_data['year']



    vehicle_node.push()

    for coverage in vehicle_data['coverages']:
        coverage_node = write_coverage_node(graph, coverage)
        results = graph.create_unique(Relationship(coverage_node, "COVERS", vehicle_node))

    return vehicle_node

def write_coverage_node(graph, coverage_data):
    print('      Writing Coverage')

    coverageId = coverage_data['id']
    coverage_node = graph.merge_one("Coverage", "coverageId", coverageId)
    coverage_node['type'] = coverage_data['type']
    coverage_node['premium'] = coverage_data['premium']
    iterator = 0
    for coverage in coverage_data['limits']:
        coverage_node['limitType'+ str(iterator)] = coverage_data['limits'][iterator]['type']
        coverage_node['limitValue' + str(iterator)] = coverage_data['limits'][iterator]['value']
        iterator = iterator + 1

    coverage_node.push()

    return coverage_node

def write_person_node(graph, type, person_data):
    print('   Writing Person')

    clientId = person_data['clientId']
    person_node = graph.merge_one("Person", "clientId", clientId)
    #quote applicant items
    person_node['age'] = person_data['age']
    person_node['birthDate'] = person_data['birthDate']
    person_node['firstName'] = person_data['firstName']
    person_node['lastName'] = person_data['lastName']
    if type == 'QUOTE_APPLICANT':
        person_node.push()
        return person_node

    person_node['gender'] = person_data['gender']
    person_node['id'] = person_data['id']
    person_node['maritalStatus'] = person_data['maritalStatus']
    person_node['phoneType'] = person_data['phoneType']


    # Applicant fields
    if 'creditReport' in person_data:
        person_node['creditReportId'] = person_data['creditReport']['id']
        person_node['creditReportReferenceNumber'] = person_data['creditReport']['referenceNumber']
        person_node['creditReportStatus'] = person_data['creditReport']['status']


    # See if this is a Driver
    if type == 'DRIVER':
        for violation in person_data['drivingRecord']['violations']:
            violation_node = write_violation_node(graph, violation)
            results = graph.create_unique(Relationship(person_node, "HAS_VIOLATION", violation_node))

        if 'licenseExpiration' in person_data:
            person_node['licenseExpiration'] = person_data['licenseExpiration']
        if 'licenseIssued' in person_data:
            person_node['licenseIssued'] = person_data['licenseIssued']
        if 'licenseState' in person_data:
            person_node['licenseState'] = person_data['licenseState']
        if 'licenseStatus' in person_data:
            person_node['licenseStatus'] = person_data['licenseStatus']
        if 'violationPoints' in person_data:
            person_node['violationPoints'] = person_data['violationPoints']
        if 'violationsAdverseAction' in person_data:
            person_node['violationsAdverseAction'] = person_data['violationsAdverseAction']

    person_node.push()

    return person_node

def write_violation_node(graph, violation_data):
    print('      Writing Violation')
    #print(violation_data)
    #violationKey = violation_data['code']+ violation_data['effectiveDate']
    violation_node = graph.merge_one("Violation", "id", violation_data['id'])
    violation_node['code'] = violation_data['code']
    if 'severity' in violation_data:
        violation_node['severity'] = violation_data['severity']
    if 'convictionDate' in violation_data:
        violation_node['convictionDate'] = violation_data['convictionDate']
    if 'effectiveDate' in violation_data:
        violation_node['effectiveDate'] = violation_data['effectiveDate']
    violation_node.push()

    return violation_node