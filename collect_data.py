# -*- coding: utf-8 -*-
"""
kNOwSurge
@author: Guanghui Liu
"""

from api_token import *
from uber_rides.session import Session
from uber_rides.client import UberRidesClient
import sys
import csv
import datetime
import time
import requests
import httplib
import urllib
import base64
import os.path

coordinates = {
'Aspen House Apartments': (38.833873, -77.062375),
'HNTB Arlington Office': (38.839864, -77.087501),
'Pentagon City Mall': (38.861529, -77.060764),
'Crystal City Metro': (38.858023, -77.05141),
'Hong Kong Pearl Dim Sum Restaurant': (38.872576, -77.152835),
'Dupont Circle': (38.909694, -77.043339),
'Union Station': (38.884868, -76.978240),
'National Mall': (38.889620, -77.022977),
}

header_0_info = ['timestamp', 'place', 'lat', 'lon']
header_1_surge = ['surge_multiplier']
header_2_weather = ['weather', 'temperature', 'pressure', 'humidity', 'wind', 'clouds', 'rain_3h', 'snow_3h']
header_3_incident = ['n_incident_red', 'n_incident_orange', 'n_incident_silver', 'n_incident_blue', 'n_incident_yellow', 'n_incident_green']

header = header_0_info + header_1_surge + header_2_weather + header_3_incident

NA = 'NA'


def get_timestamp():
    return str(datetime.datetime.now())


def get_surge(lat, lon):
    result = {}    
    result['surge_multiplier'] = NA
    
    try:
        session = Session(server_token=my_uber_server_token)
        client = UberRidesClient(session)
        response = client.get_price_estimates(lat, lon, lat+0.05, lon+0.05) #create a minor jitter
        
        product0 = response.json['prices'][0]    
        assert(product0['display_name'] == 'uberX')
        result['surge_multiplier'] = product0['surge_multiplier']
        
    except Exception as e:
        print str(e)
        
    return result


def get_surge_values(lat, lon):
    return get_surge(lat, lon)['surge_multiplier']
    
    
def get_weather(lat, lon):
    result = {}
    result['weather'] = NA
    result['temperature'] = NA
    result['pressure'] = NA
    result['humidity'] = NA
    result['wind'] = NA
    result['clouds'] = NA 
    result['rain_3h'] = NA
    result['snow_3h'] = NA    
    
    try:
        api_string = 'http://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&APPID={}&units={}'.format(lat, lon, my_weather_api_key, 'metric')
        response = requests.get(api_string)
        record = response.json()

        result['weather'] = record['weather'][0]['description']
        result['temperature'] = record['main']['temp'] # temperature, Celsius
        result['pressure'] = record['main']['pressure'] # atmospheric pressure, hPa
        result['humidity'] = record['main']['humidity'] # humidity, %
        
        if 'wind' in record:
            result['wind'] = record['wind']['speed'] # wind speed, m/s
        if 'clouds' in record:
            result['clouds'] = record['clouds']['all'] # cloudiness, %
        if 'rain' in record:
            result['rain_3h'] = record['rain'].get('3h', NA) # rain volume for the last 3hrs, ?
        if 'snow' in record:
            result['snow_3h'] = record['snow'].get('3h', NA) # snow volume for the last 3hrs, ?
        
    except Exception as e:
        print str(e)
        
    return result


def get_weather_values(lat, lon):
    result = get_weather(lat, lon)
    return [value for (key, value) in sorted(result.items(), key=lambda item: header_2_weather.index(item[0]))]    
    

def get_wmata_incident():
    result = {}
    result['n_incident_red'] = NA
    result['n_incident_orange'] = NA
    result['n_incident_silver'] = NA
    result['n_incident_blue'] = NA
    result['n_incident_yellow'] = NA
    result['n_incident_green'] = NA 
    try:
        api_string = 'https://api.wmata.com/Incidents.svc/json/Incidents'
        response = requests.get(api_string, headers={'api_key': my_wmata_api_key})
        record = response.json()
        
        incidents = record['Incidents']
        affected = " ".join([incident['LinesAffected'] for incident in incidents])
        result['n_incident_red'] = affected.count('RD')
        result['n_incident_orange'] = affected.count('OR')
        result['n_incident_silver'] = affected.count('SV')
        result['n_incident_blue'] = affected.count('BL')
        result['n_incident_yellow'] = affected.count('YL')
        result['n_incident_green'] = affected.count('GR')
        
    except Exception as e:
        print str(e)
        
    return result


def get_wmata_incident_values():
    result = get_wmata_incident()
    return [value for (key, value) in sorted(result.items(), key=lambda item: header_3_incident.index(item[0]))]    


def collect_data():
    records = []
    incident = get_wmata_incident_values()
    
    for place, latlon in coordinates.iteritems():
        record = []

        lat, lon = latlon[0], latlon[1]
        surge = get_surge_values(lat, lon)
        weather = get_weather_values(lat, lon)
        
        record.append(get_timestamp())
        record.append(place)
        record.extend(latlon)
        record.append(surge)
        record.extend(weather)
        record.extend(incident)
        
        records.append(record)
    return records


def collect_data_continuous(filename, t_interval = 60, t_total = 60*60*24*30):
    #if file does not exist, write header
    if not os.path.isfile(filename):
        with open(filename, 'ab') as f:
            writer = csv.writer(f)
            writer.writerow(header)

    t = 0
    while t < t_total:  
        t += t_interval
        
        t_session_start = time.time()
        print "Fetching...", t, '/', t_total
        
        records = collect_data()
        print records
        
        with open(filename, 'ab') as f:
            writer = csv.writer(f)
            writer.writerows(records)
        
        t_session_end = time.time() 
        time.sleep(t_interval - (t_session_end - t_session_start) % t_interval)


if __name__ == '__main__':
    collect_data_continuous('records.csv')
