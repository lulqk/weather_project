import pandas as pd
from scratchpad import location_and_station_per_dataid, check_if_holiday, get_location_and_station_per_dataid
from weather import get_weather
from api_keys import METADATA_PATH, AUSTIN_15_PATH, CALI_15_PATH, NY_15_PATH
import time
from feature_eng import feature_eng_dataset


STATES = {
    'Texas': 'TX',
    'California': 'CA',
    'Newy York': 'NY',
    'Colorado': 'CO',
    'Illinois': 'IL',
    'Oklahoma': 'OK',
    'Maryland': 'MD'}


def trim_time(val):
    return val[:-3]


def pipeline(df_city, df_metadata):

    # Kopia wybranych kolumn z oryginalnego zbioru
    print('Kopiowanie danych')
    working_city = df_city[['dataid', 'local_15min', 'grid']].copy()

    # Usunięcie wsyzstkich wierszy zawierających NA (odpada 0,005% zbioru)
    print('Usuwanie "NA"')
    working_city = working_city.dropna()

    # Stworzenie kolumny 'city' na podstawie zbioru metadanych
    print("Tworzenie kolumny 'city'")
    working_city['city'] = working_city.apply(
        lambda row: df_metadata.loc[df_metadata['dataid'] == row['dataid'], 'city'], axis=1)

    # Stworzenie kolumny 'state' na podstawie zbioru metadanych
    print("Tworzenie kolumny 'state'")
    working_city['state'] = working_city.apply(
        lambda row: df_metadata.loc[df_metadata['dataid'] == row['dataid'], 'state'], axis=1)

    # Poprawienie wartosci czasu, a nastepnie zmiana typu danych na timestamp
    print("Poprawienie czasu")
    working_city.local_15min = working_city.local_15min.apply(trim_time)
    working_city.local_15min = pd.to_datetime(working_city.local_15min)

    # Pobranie stacji pogodowych i danych geolokalizacyjnych dla każdego miasta
    print("Pobieranie danych geolokalizacyjnych")
    cities_meta = get_location_and_station_per_dataid(df_metadata, working_city)
    print(cities_meta)

    # Stworzenie kolumn i pobranie wartości 'station_id', 'latitude', 'longitude'
    print("Tworzenie kolumn ze stacja i lokalizacja")
    location_frame = working_city.apply(
        lambda x: pd.Series(
            location_and_station_per_dataid(str(x['local_15min'].date()),cities_meta), index=['station_id', 'latitude', 'longitude']),
        axis=1,
        result_type='expand')

    working_city = pd.concat([working_city, location_frame], axis=1)

    print(working_city)

    # Stworznenie kolumn i pobranie wartości tempAvg, windspeedAvg, pressureMax, humidityAvg, winddirAvg
    print("Tworzenie kolumn z danymi pogodowymi")
    weather_frame = working_city.apply(
        lambda x: pd.Series(
            get_weather(x['local_15min'], x['station_id']),
            index=['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg']),
        axis=1,
        result_type='expand')

    working_city = pd.concat([working_city, weather_frame], axis=1)

    # Dodanie kolumny, która sprawdza czy dzień był wolny od pracy (weekendy i święta)
    working_city['holiday'] = working_city.apply(
        lambda row: check_if_holiday(row.local_15min.date(), STATES.get(row.state)), axis=1)

    print("Koniec")

    return working_city


def pipeline_per_house(df, df_metadata):
    start = time.time()
    result = pipeline(df, df_metadata)
    print(result.info())
    print(result.describe())
    end = time.time()
    print("\nUpłyneło: ", end - start)
    return result


def main(dataset, name):
    start = time.time()
    df = pd.read_csv(dataset)
    df_metadata = pd.read_csv(METADATA_PATH)
    ids = df.dataid.unique().copy()
    done = [661, 1642, 2335]

    for dataid in ids:
        if dataid in done:
            pass
        else:
            print("Tworzenie dataframe dla ID: ", dataid)
            sub_df = df.loc[df.dataid == dataid].copy()
            result = pipeline_per_house(sub_df, df_metadata)
            result = result.reset_index()
            result.to_csv('data/' + name + '/pipeline_' + str(dataid) + '_' + name + '.csv')
            feature_eng_dataset(result, name)
    end = time.time()
    print("\nCałkowity czas: ", end - start)


main(AUSTIN_15_PATH, 'austin')
