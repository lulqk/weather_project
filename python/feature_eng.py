import pandas as pd
from sklearn.preprocessing import PolynomialFeatures, MinMaxScaler
from sklearn.feature_selection import SelectKBest, f_regression


def get_hour(time):
    return pd.to_datetime(time).hour


def get_polynomial_features(data):
    poly = PolynomialFeatures(degree=3, include_bias=False)
    features_to_transform = data[['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg']].copy()
    features_transformed = poly.fit_transform(features_to_transform)
    col_names = poly.get_feature_names(['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg'])
    new_features = pd.DataFrame(features_transformed, columns=col_names)
    new_features = new_features.drop(['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg'], axis=1)
    new_df = pd.concat([data, new_features], axis=1)

    return new_df


def select_best_features(dataset):
    # Usunięcie wartości NA z datasetu
    data = dataset.dropna()
    # Pobranie zmiennej celu
    dt_grid = data.grid.copy()
    # Przekszałtecenie array z 1D na 2D
    dt_grid = dt_grid.values.reshape(-1, 1)
    # Przeskalowanie zmiennej na wartości od 0 do 1
    dt_grid_scaled = MinMaxScaler().fit_transform(dt_grid)

    dt_numeric = data.drop(['Unnamed: 0', 'dataid', 'local_15min', 'city', 'state',
                            'station_id', 'latitude', 'longitude', 'grid'], axis=1).copy()
    dt_numeric_scaled = MinMaxScaler().fit_transform(dt_numeric)

    # Wybór najlepszych cech metodą f_regression
    best_features = SelectKBest(f_regression, k=10).fit(dt_numeric_scaled, dt_grid_scaled)
    df_scores = pd.DataFrame(best_features.scores_)
    df_columns = pd.DataFrame(dt_numeric.columns)
    feature_scores = pd.concat([df_columns, df_scores], axis=1)
    feature_scores.columns = ['Feature', 'Score']

    best_features_set = pd.DataFrame()
    for col in feature_scores.nlargest(10, 'Score')['Feature']:
        best_features_set[col] = dataset[col].copy()

    dt_descriptive = dataset[['dataid', 'local_15min', 'city', 'state',
                          'station_id', 'latitude', 'longitude']].copy()

    return pd.concat([dt_descriptive, best_features_set, dataset.grid.copy()], axis=1)


def feature_eng_dataset(dataset):
    dataid = dataset.dataid[0]
    # Usunięcie N/A
    data = dataset.dropna()
    # Dodanie kolumny z godziną pomiaru
    data['hour'] = data.local_15min.apply(get_hour)
    # Utworzenie zmiennych wielomianowych na podstawie numerycznych zmiennych pogodowych
    data = get_polynomial_features(data)
    # Wybranie 10 najlepszych zmiennych numerycznych
    data = select_best_features(data)
    data.to_csv('data/austin/best_features_' + str(dataid) + '.csv', index=False)


feature_eng_dataset(pd.read_csv('data/austin/pipeline_661_austin.csv'))