#!/usr/bin/env python
import sys
import json
from pprint import pprint


filename = 'automatic.txt'
infile = open(filename, 'r')

with open(filename) as data_file:
    json_data = json.load(data_file)
    '''
    if json_data['type'] == 'events.quote.QuoteCreated':
        print(json_data['event']['quote']['address']['city'])
        '''
print json.dumps(data, sort_keys=True,indent=4)
#print data["new_policy"]["vehicles"][0]["model"]

#pprint(data)
#for line in infile:
#    json_line = json.load(infile)
#    print json.dumps(json_line, sort_keys=True, indent=4)
#    print '_____________________________________________'

