import requests
from datetime import datetime, date
import pandas as pd
import os
from weather import download_weather_data, create_date
from api_keys import *
import holidays


def load_metadata():
    df_metadata = pd.read_csv(METADATA_PATH)
    return df_metadata


def geolocation_per_city(city, state):
    location = city + ' ' + state
    api_key = GOOGLE_API
    payload = {'address': location,
               'key': api_key}

    headers = {'Accept-Encoding': 'gzip'}
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    r = requests.get(url=url, params=payload, headers=headers)
    result = r.json()
    latitude = round(result['results'][0]['geometry']['location']['lat'], 2)
    longitude = round(result['results'][0]['geometry']['location']['lng'], 2)

    return [latitude, longitude]


def get_user_data():
    # Odczyt danych dotyczących użytkowników Pecan Street
    if os.path.exists('data/user_data.csv'):
        return pd.read_csv('data/user_data.csv')
    else:
        metadata = load_metadata()
        df = pd.DataFrame(columns=['dataid', 'date_enrolled'])
        df.dataid = metadata.dataid.unique().copy()
        df.date_enrolled = df.apply(lambda row: metadata.loc[metadata['dataid'] == row['dataid'], 'date_enrolled'].values[0], axis=1)


def get_valid_station_by_geolocation(lat, lng, min_date):
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
            if download_weather_data(create_date(min_date), station_id) == 200:
                return station_id

    return 'None'


def get_location_and_station_per_city(min_date):

    # sprawdzenie czy istnieje już plik z danymi geolokalizacyjnymi
    if os.path.exists('data/geolocation.csv'):
        return pd.read_csv('data/geolocation.csv')
    else:
        # Pobranie metadanych
        metadata = load_metadata()
        # Utworzenie dataframe zawierającego nazwe miasta, stan, dlugość i szerokość gograficzną, id stacji pogodowej
        df = pd.DataFrame(columns=['city', 'state', 'lat', 'lng', 'station_id'])
        df.city = metadata.city.unique().copy()
        df['state'] = df.apply(lambda row: metadata.loc[metadata['city'] == row['city'], 'state'].values[0], axis=1)
        for index, row in df.iterrows():
            row['lat'], row['lng'] = geolocation_per_city(row['city'], row['state'])
            row['station_id'] = get_valid_station_by_geolocation(row['lat'], row['lng'], min_date)

        # Zapisanie danych do pliku
        df.to_csv('data/geolocation.csv')

        return df


def location_and_station(city, state, cities_frame):
    return cities_frame.loc[(cities_frame['city']==city) & (cities_frame['state']==state), ['station_id', 'lat', 'lng']].values[0]


def check_if_holiday(date, state):
    state_holidays =  holidays.CountryHoliday('US', state=state)
    holiday = False
    if date in state_holidays:
        holiday = True
    weekend = False
    if date.weekday() > 4:
        weekend = True

    if holiday or weekend:
        return True
    else:
        return False


def to_date(timestamp):
    return timestamp.date()


def get_valid_station_by_geolocation_per_dataid(lat, lng, date):
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
            if download_weather_data(str(date).replace('-', ''), station_id) == 200:
                return station_id

    return 'None'


def get_location_and_station_per_dataid(metadata, id_full_df):

    dataid = id_full_df.dataid.values[0]

    # sprawdzenie czy istnieje już plik z danymi geolokalizacyjnymi
    if os.path.exists('data/geolocation'+str(dataid)+'.csv'):
        return pd.read_csv('data/geolocation'+str(dataid)+'.csv')
    else:
        # Utworzenie dataframe zawierającego date, nazwe miasta, stan, dlugość i szerokość gograficzną, id stacji pogodowej
        df = pd.DataFrame(columns=['date', 'city', 'state', 'lat', 'lng', 'station_id'])
        # Kopia i odzyskanie unikalnych dat z pliku źródłowego
        dates = id_full_df.local_15min.copy()
        dates = dates.apply(to_date)
        df.date = dates.unique()
        # Pobranie nazwy miasta i stanu
        city = metadata.loc[metadata.dataid == dataid].city.values[0]
        state = metadata.loc[metadata.dataid == dataid].state.values[0]
        # Uzupełnienie dataframe
        for index, row in df.iterrows():
            row['city'] = city
            row['state'] = state
            row['lat'], row['lng'] = geolocation_per_city(row['city'], row['state'])
            row['station_id'] = get_valid_station_by_geolocation_per_dataid(row['lat'], row['lng'], row['date'])

        # Zapisanie danych do pliku
        df.to_csv('data/geolocation'+str(dataid)+'.csv')

        return df


def location_and_station_per_dataid(date, cities_frame):
    return cities_frame.loc[cities_frame['date']==date, ['station_id', 'lat', 'lng']].values[0]
