
import requests
import json


def get_geocode(address_json):
    print('     Getting Geocode')
    geo_address = address_json['street']+address_json['city']+','+address_json['state']
    key = "AIzaSyBvAef_lw2NB3HgXGoeaVyURlOhK96tZJ0"
    params = {'address': geo_address, 'key': key}
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    response = requests.get(url, params=params, verify=False)
    print(response.url)
    if response.status_code != 200:
        print('Get Geocode status: %s' % response.status_code)
    else:
        geo_address = json.loads(response.text)
        #print(geo_address['results'][0]['geometry']['location']['lat'])
        return geo_address['results'][0]['geometry']['location']['lat'],  geo_address['results'][0]['geometry']['location']['lng']

def main():
    address_json = {}
    address_json['street'] = '5601 Majestic Circle'
    address_json['city'] = 'Columbia'
    address_json['state'] = 'MO'
    get_geocode(address_json)

# Start program
if __name__ == "__main__":
   main()