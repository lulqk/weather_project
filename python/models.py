import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.metrics import mean_squared_error
import numpy as np
from sklearn.linear_model import LinearRegression, Lasso, LassoCV
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor


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


def grid_regression(data):

    # Stworzenie zbioru treningowego i testowego
    train_set, test_set = train_test_split(data, test_size=0.2, random_state=42)

    # Stworzenie zestawów cech i etykiet
    X_train = train_set.loc[:, :'hour^3']
    y_train = train_set['grid']
    X_test = test_set.loc[:, :'hour^3']
    y_test = test_set['grid']

    # Model regresji liniowej
    lin_reg = LinearRegression()
    lin_reg.fit(X_train, y_train)
    lin_reg_result = train_eval_model(lin_reg, "Linear Regressor", X_train, y_train, X_test, y_test)

    # Model drzewa decyzyjnego
    tree_reg = DecisionTreeRegressor()
    tree_reg.fit(X_train, y_train)
    tree_result = train_eval_model(tree_reg, "Decision Tree Regressor", X_train, y_train, X_test, y_test)

    # Model Lasu Losowego
    forest_reg = RandomForestRegressor(random_state=42)
    forest_reg.fit(X_train, y_train)
    forest_result = train_eval_model(forest_reg, "Random Forest Regressor", X_train, y_train, X_test, y_test)

    # Wyznaczenie najlepszego lasu losowego przez przeszukanie siatki
    # Siatka hiperparametrów
    param_grid = [
       {'n_estimators': [50, 100], 'max_features': [4, 8]},
    ]
    forest_grid = GridSearchCV(RandomForestRegressor(random_state=42),
                               param_grid, cv=10,
                               scoring="neg_mean_squared_error",
                               n_jobs=-1)
    forest_grid.fit(X_train, y_train)
    forest_grid_result = train_eval_model(forest_grid, "RF GSCV Regressor", X_train, y_train, X_test, y_test)

    # Metoda Lasso
    lasso = Lasso()
    lasso.fit(X_train, y_train)
    lasso_result = train_eval_model(lasso, "Lasso Regressor", X_train, y_train, X_test, y_test)

    # Metoda LassoCV - dopasowanie Lasso przez kroswalidacje
    lasso_cv = LassoCV(random_state=42)
    lasso_cv.fit(X_train, y_train)
    lasso_cv_result = train_eval_model(lasso_cv, "LassoCV Regressor", X_train, y_train, X_test, y_test)

    final_results = {
        "LinearRegression": lin_reg_result,
        "DecisionTreeRegressor": tree_result,
        "RandomForestRegressor": forest_result,
        "RandomForestGSCVRegressor": forest_grid_result,
        "LassoRegression": lasso_result,
        "LassoCVRegression": lasso_cv_result
    }
    return final_results


def prepare_data(dataset):
    data = dataset.dropna()
    data = data.drop(['dataid', 'local_15min', 'city', 'state', 'station_id', 'latitude', 'longitude'], axis=1)
    return data


data = prepare_data(pd.read_csv('data/austin/best_features_661.csv'))
results = grid_regression(data)
print_regression_results(results)