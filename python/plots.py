import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd


def get_correlation_plot(data, name='correlation_matrix'):
    # Stworzenie macierzy korelacji
    corr_matrix = data.corr()
    # Stworzenie i zapisanie do pliku heatmapy korelacji
    mask = np.zeros_like(corr_matrix)  # Maska - do przykrycia górnej cześći wykresu
    mask[np.triu_indices_from(mask)] = True
    f, ax = plt.subplots(figsize=(20, 20))  # figsize - określa rozmiar generowanego wykresu
    with sns.axes_style("white"):
        corr_plot = sns.heatmap(corr_matrix.round(2), mask=mask, square=True, annot=True, linewidths=.5, ax=ax)
    fig = corr_plot.get_figure()
    fig.savefig('data/plots/' + name + '.png')  # zapisanie wykresu do pliku


def get_pairplot(data, name='pair_plot'):
    pp = sns.pairplot(data)
    pp.savefig('data/plots/' + name + '.png')


def get_histograms(data, dataid):
    columns = ['grid', 'temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg']
    for col in columns:
        plt.hist(data[col])
        plt.title(col + " for ID: " +  str(dataid))
        plt.savefig('data/plots/hist_' + col + "_" + str(dataid) + '.png')
        plt.close()


def get_scatter_plots(data, dataid):
    columns = ['grid', 'temp_avg', 'wind_speed_avg', 'wind_dir_avg', 'pressure_max', 'humidity_avg']
    time = data.local_15min
    start = time.min()
    end = time.max()
    for col in columns:
        plt.figure(figsize=(40, 40))
        plt.scatter(time, data[col])
        plt.title(col + " for ID: " + str(dataid) + " " + start + ' - ' + end)
        plt.savefig('data/plots/scatter_' + col + '_' + str(dataid)+ '.png')
        plt.close()


data = pd.read_csv('data/austin/best_features_661.csv')

get_pairplot(data)
