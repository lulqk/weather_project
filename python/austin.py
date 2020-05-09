import pandas as pd
from scratchpad import get_location_and_station_per_city, location_and_station
from weather import get_weather
from api_keys import METADATA_PATH, AUSTIN_15_PATH


def trim_time(time):
    return time[:-3]


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
    cities_meta = get_location_and_station_per_city()

    # Stworzenie kolumn i pobranie wartości 'station_id', 'latitude', 'longitude'
    print("Tworzenie kolumn ze stacja i lokalizacja")
    location_frame = working_city.apply(
        lambda x: pd.Series(
            location_and_station(x['city'], x['state'], cities_meta), index=['station_id', 'latitude', 'longitude']),
        axis=1,
        result_type='expand')

    working_city = pd.concat([working_city, location_frame], axis=1)

    print(working_city)
    # Stworznenie kolumn i pobranie wartości tempAvg, windspeedAvg, pressureMax, humidityAvg, winddirAvg
    print("Tworzenie kolumn z danymi pogodowymi")
    weather_frame = working_city.apply(
        lambda x: pd.Series(
            get_weather(x['local_15min'], x['station_id']),
            index=['temp_avg', 'wind_speed_avg', 'pressure_max', 'humidity_avg', 'wind_dir_avg']),
        axis=1,
        result_type='expand')

    working_city = pd.concat([working_city, weather_frame], axis=1)
    print("Koniec")

    return working_city


def pipeline_trial():
    df_austin = pd.read_csv(AUSTIN_15_PATH)
    df_metadata = pd.read_csv(METADATA_PATH)

    x = df_austin[:1000].copy()
    result = pipeline(x, df_metadata)
    print(result)
    print(result.info())
    print(result.describe())


pipeline_trial()
