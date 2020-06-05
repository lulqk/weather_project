import pandas as pd
import numpy as np
from sklearn.preprocessing import PolynomialFeatures, MinMaxScaler
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.compose import TransformedTargetRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import LinearSVR
from sklearn.neural_network import MLPRegressor
from sklearn.linear_model import SGDRegressor
import time


def print_regression_results(results):
    """Wyświetlenie na ekranie infomacji o wynikach danego regresora """
    for name, model in results.items():
        print("\n" + model.model_name + ":\n")
        print("\tBłąd RMSE trening: " + str(model.rmse_train))
        print("\tBłąd RMSE test: " + str(model.rmse_test))
        print("\tWyniki kroswalidacji:\n")
        print("\t\tŚrednia: " + str(model.cvs_mean))
        print("\t\tOdchylenie standardowe: " + str(model.cvs_std))
        print("\t\tWyniki: " + str(model.cvs_scores))


class ResultDataRegressors:
    """Klasa w której zapisuje wynik każdego algorytmu"""

    def __init__(self, model_name, model, rmse_train, rmse_test, cvs_scores):
        self.model_name = model_name
        self.model = model
        self.rmse_train = rmse_train
        self.rmse_test = rmse_test
        self.cvs_scores = cvs_scores

        self.cvs_mean = self.cvs_scores.mean()
        self.cvs_std = self.cvs_scores.std()


def train_eval_model(model, model_name, X_train, y_train, X_test, y_test):
    """ Funkcja przeprowadza trening i ewaluacje modelu.
        Zwraca obiek klasy ResultDataRegressor"""

    model_predictions_train = model.predict(X_train)  # Wyniki regresji dla zbioru treningowego
    model_mse_train = mean_squared_error(y_train, model_predictions_train)  # MSE dla zbioru treningowego
    model_rmse_train = np.sqrt(model_mse_train)  # RMSE dla zbioru treningowego
    model_predictions_test = model.predict(X_test)
    model_mse_test = mean_squared_error(y_test, model_predictions_test)
    model_rmse_test = np.sqrt(model_mse_test)
    # Kroswalidacja modelu
    model_scores = cross_val_score(model, X_train, y_train, scoring="neg_mean_squared_error", cv=10)
    model_rmse_scores = np.sqrt(-model_scores)

    model_result = ResultDataRegressors(model_name, model, model_rmse_train, model_rmse_test, model_rmse_scores)
    return model_result


def regression(data):

    # Stworzenie zbioru treningowego i testowego
    train_set, test_set = train_test_split(data, test_size=0.2, random_state=42)

    # Stworzenie zestawów cech i etykiet
    X_train = train_set.loc[:, :'pressure_max^3']
    y_train = train_set['grid']
    X_test = test_set.loc[:, :'pressure_max^3']
    y_test = test_set['grid']

    # print("regresja liniowa")
    # # Model regresji liniowej
    # lin_reg = TransformedTargetRegressor(regressor=LinearRegression(), transformer=MinMaxScaler())
    # lin_reg.fit(X_train, y_train)
    # lin_reg_result = train_eval_model(lin_reg, "Transformed Linear Regressor", X_train, y_train, X_test, y_test)
    # print("SGD")
    # # SGD
    # sgd_reg = TransformedTargetRegressor(regressor=SGDRegressor(), transformer=MinMaxScaler())
    # sgd_reg.fit(X_train, y_train)
    # sgd_reg_result = train_eval_model(sgd_reg, "Transformed SGD Regressor", X_train, y_train, X_test, y_test)
    print("las losowy n_estimators = 500")
    # Model Lasu Losowego
    forest_reg = TransformedTargetRegressor(regressor=RandomForestRegressor(random_state=42, n_jobs=-1, n_estimators=500), transformer=MinMaxScaler())
    forest_reg.fit(X_train, y_train)
    forest_result = train_eval_model(forest_reg, "Random Forest Regressor", X_train, y_train, X_test, y_test)
    # print('svr')
    # # SVR
    # svm_reg = TransformedTargetRegressor(regressor=LinearSVR(random_state=42, dual=False), transformer=MinMaxScaler())
    # svm_reg.fit(X_train, y_train)
    # svm_results = train_eval_model(svm_reg, 'SVM-rbf', X_train, y_train, X_test, y_test)
    # print('nn')
    # # NN
    # nn_reg = TransformedTargetRegressor(regressor=MLPRegressor(random_state=42), transformer=MinMaxScaler())
    # nn_reg.fit(X_train, y_train)
    # nn_results = train_eval_model(nn_reg, 'NN', X_train, y_train, X_test, y_test)


    final_results = {
        "RandomForestRegressor": forest_result,
        # "SvmRbfRegressor": svm_results,
        # "MLPRegressor": nn_results
    }
    return final_results

print('Start!')
start = time.time()
# Import pliku do csv
df = pd.read_csv('data/data_all_numeric.csv', index_col=0)

# Wyciągnięcie zmiennej z godziną pomiaru
df.local_1hour = pd.to_datetime(df.local_1hour)
df['hour'] = df.local_1hour.apply(lambda row: row.hour)

# Wyciągnięcie zmiennej z dniem roku
df['dayofyear'] = df.local_1hour.apply(lambda row: row.dayofyear)

# Usunięcie zmiennej z dokładnym czasem
df = df.drop('local_1hour', axis=1)

# Usunięcie szerokości i długości geograficznych - są tylko 2 różne zestawy i korespondują ze zmienną 'state'
df = df.drop(['lat', 'lng'], axis=1)

# Usunięcie NA
df = df.dropna()
print('Tworzenie nowych zmiennych.')
# Stworzenie nowych zmiennych za pomocą PolynomialFeatures
poly = PolynomialFeatures(degree=3, include_bias=False)
features_to_transform = df[
    ['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg', 'hour', 'dayofyear',
     'total_square_footage']].copy()
features_transformed = poly.fit_transform(features_to_transform)
col_names = poly.get_feature_names(
    ['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg', 'hour', 'dayofyear',
     'total_square_footage'])
new_features = pd.DataFrame(features_transformed, columns=col_names)
new_features = new_features.drop(['temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg', 'hour', 'dayofyear',
     'total_square_footage'],axis=1)
new_df = pd.concat([df, new_features], axis=1)
print('Wybieranie najlepszej zmiennej.')
# Wybranie najlepszych zmiennych za pomocą funkcji SelectKMax

new_df = new_df.dropna()
df_grid = new_df.grid.copy()
df_grid = df_grid.values.reshape(-1,1)

scaler = MinMaxScaler()
scaler.fit(df_grid)

df_gscaled = scaler.transform(df_grid)
df_numeric = new_df.drop('grid', axis=1).copy()
df_numscaled = scaler.transform(df_numeric)
best_features = SelectKBest(f_regression, k=20).fit(df_numscaled, df_gscaled)

df_scores = pd.DataFrame(best_features.scores_)
df_columns = pd.DataFrame(df_numeric.columns)
feature_scores = pd.concat([df_columns, df_scores], axis=1)
feature_scores.columns = ['Feature', 'Score']

best_features_set = pd.DataFrame()
tmp_cols = df_numeric.columns
df_numscaled = pd.DataFrame(df_numscaled, columns=tmp_cols)

for col in feature_scores.nlargest(20, 'Score')['Feature']:
    best_features_set[col] = df_numeric[col].copy()

tmp_df = pd.concat([best_features_set, pd.DataFrame(df_grid, columns=['grid',])], axis=1)
tmp_df = tmp_df.dropna()

print('Models start.')

reg = regression(tmp_df)
print_regression_results(reg)
end = time.time()

print('Czas trwania:', end-start)

