import requests
import json
import os
from datetime import datetime
import numpy as np
import pandas as pd
from api_keys import *


def create_date(timestamp):
    year = str(timestamp.year)
    if timestamp.month < 10:
        month = '0' + str(timestamp.month)
    else:
        month = str(timestamp.month)
    if timestamp.day < 10:
        day = '0' + str(timestamp.day)
    else:
        day = str(timestamp.day)
    date = year + month + day
    return date


def get_weather_data(date, station_id):
    frmt = 'json'
    units = 'm'
    api_key = WEATHER_API
    url = 'https://api.weather.com/v2/pws/history/hourly'

    headers = {'Accept-Encoding': 'gzip, deflate, br'}

    payload = {'stationId': station_id, 'format': frmt, 'units': units, 'date': date, 'apiKey': api_key}
    r = requests.get(url=url, params=payload, headers=headers)

    return r


def get_new_station(date, lat, lng):
    geocode = str(lat) + ',' + str(lng)
    product = 'pws'
    frmt = 'json'
    api_key = WEATHER_API
    payload = {'geocode': geocode, 'product': product, 'format': frmt, 'apiKey': api_key}

    headers = {'Accept-Encoding': 'gzip, deflate, br'}

    url = 'https://api.weather.com/v3/location/near'

    r = requests.get(url=url, params=payload, headers=headers)
    response = r.json()
    stations = response['location']['qcStatus']
    for index, value in enumerate(stations):
        if value == 1:
            station_id = response['location']['stationId'][index]
            new_station = get_weather_data(date, station_id)
            if new_station.status_code == 200:
                return new_station

    return 'None'


def download_weather_data(date, station_id, lat=0, lng=0):
    r = get_weather_data(date, station_id)

    if r.status_code == 200:
        result = r.json()
        filepath = "weather_data/" + date + "_" + station_id + ".json"

        with open(filepath, "w+") as outfile:
            json.dump(result, outfile)
    else:
        if r.status_code == 401:
            print("Klucz API został zablokowany")
        else:
            print("Station: " + station_id + " date: " + date + " code:" + str(r.status_code))
        # new_result = get_new_station(date, lat, lng).json()
        # new_station = new_result['observations'][0]['stationID']
        #
        # filepath = "weather_data/" + date + "_" + new_station + ".json"
        #
        # with open(filepath, "w+") as outfile:
        #     json.dump(new_result, outfile)
        #
        # geolocation = pd.read_csv('data/geolocation.csv')
        # geolocation.loc[geolocation['station_id']==station_id, 'station_id'] = new_station
        # geolocation.to_csv('data/geolocation.csv', index=False)
        # print("New station: " + new_station + "for date: " + date)

    return r.status_code


# Parametry pobierane z API: tempAvg, windspeedAvg, pressureMax, humidityAvg, winddirAvg
def get_weather(timestamp, station_id, lat, lng):

    if station_id == 'None':
        return [np.nan, np.nan, np.nan, np.nan, np.nan]

    date = create_date(timestamp)
    path = "weather_data/" + date + "_" + station_id + ".json"

    if not os.path.exists(path):
        download_weather_data(date, station_id, lat=lat, lng=lng)

    try:
        with open(path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return [np.nan, np.nan, np.nan, np.nan, np.nan]

    try:
        result = data['observations']
        for observation in result:
            hour = datetime.strptime(observation['obsTimeLocal'], '%Y-%m-%d %H:%M:%S').hour
            if hour == timestamp.hour:
                temp_avg = observation['metric']['tempAvg']
                wind_speed_avg = observation['metric']['windspeedAvg']
                wind_dir_avg = observation['winddirAvg']
                pressure_max = observation['metric']['pressureMax']
                humidity_avg = observation['humidityAvg']

                return [temp_avg, wind_speed_avg, wind_dir_avg, pressure_max, humidity_avg]
    except KeyError:
        return [np.nan, np.nan, np.nan, np.nan, np.nan]
