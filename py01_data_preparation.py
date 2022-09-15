import lib.data.clean as clean
import lib.data.raw as raw
import lib.data as data

from lib.utils import normalize_string

import pandas as pd
import numpy as np

import os


def main():

    # Load RAW csv files
    dataset_file_names = pd.read_csv('data/datasets_madrid.csv')['Dataset name'].tolist()

    datasets = list(map(raw.read_dataset_from_raw, dataset_file_names))

    #########################################################
    # Air quality data
    print('Air quality data')
    measurement_stations, dataset = datasets[0:2]

    measurement_stations = measurement_stations[['CODIGO_CORTO', 'LONGITUD', 'LATITUD']]
    dataset = dataset.drop(columns=['PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO'])

    # filter by year
    dataset = dataset[dataset['ANO'] >= data.START_YEAR]

    joined_data = pd.merge(left=dataset, 
                           right=measurement_stations,
                           left_on='ESTACION',
                           right_on='CODIGO_CORTO',
                           how='inner').drop(columns=['ESTACION', 'CODIGO_CORTO'])
    
    hour_columns = r'H\d\d'
    valid_columns = r'V\d\d'

    joined_data = clean.multi_melt(
                                joined_data, 
                                [hour_columns, valid_columns],
                                [('hour', 'measure'),('valid', 'is_valid')])

    rename_cols = {
        'ANO': 'year',
        'MES': 'month',
        'DIA': 'day',
        'hour': 'hour',
        'MAGNITUD': 'magnitude',
        'measure': 'measure',
        'is_valid': 'is_valid',
        'LONGITUD': 'longitude',
        'LATITUD': 'latitude'
    }

    joined_data = joined_data.drop(columns=['valid']).rename(columns=rename_cols)
    joined_data = joined_data[list(rename_cols.values())]

    remove_H_prefix = lambda x: int(x[1:]) - 1
    joined_data['hour'] = np.vectorize(remove_H_prefix)(joined_data['hour'])

    valid_to_bool = lambda x: x == 'V'
    joined_data['is_valid'] = np.vectorize(valid_to_bool)(joined_data['is_valid'])

    if not os.path.exists(data.CLEAN_DIR):
        os.mkdir(data.CLEAN_DIR)
    
    joined_data.to_csv(f'{data.CLEAN_DIR}air_quality.csv', index=False)

    #########################################################
    # Meteorological data
    print('Meteorological data')

    measurement_stations, dataset = datasets[2:4]

    measurement_stations = measurement_stations[['CÓDIGO_CORTO', 'LONGITUD', 'LATITUD']]
    dataset = dataset.drop(columns=['PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO'])

    # filter by year
    dataset = dataset[dataset['ANO'] >= data.START_YEAR]

    joined_data = pd.merge(left=dataset, 
                           right=measurement_stations,
                           left_on='ESTACION',
                           right_on='CÓDIGO_CORTO',
                           how='inner').drop(columns=['ESTACION', 'CÓDIGO_CORTO'])

    joined_data = clean.multi_melt(
                                joined_data, 
                                [hour_columns, valid_columns],
                                [('hour', 'measure'),('valid', 'is_valid')])

    joined_data = joined_data.drop(columns=['valid']).rename(columns=rename_cols)
    joined_data = joined_data[list(rename_cols.values())]

    joined_data['hour'] = np.vectorize(remove_H_prefix)(joined_data['hour'])
    joined_data['is_valid'] = np.vectorize(valid_to_bool)(joined_data['is_valid'])
    
    joined_data.to_csv(f'{data.CLEAN_DIR}meteorological_data.csv', index=False)

    #########################################################
    # Noise data
    print('Noise data')

    measurement_stations, dataset = datasets[4:6]

    measurement_stations = measurement_stations[['Nº', 'LONGITUD_WGS84', 'LATITUD_WGS84']]

    # filter by year
    dataset = dataset[dataset['anio'] >= data.START_YEAR]

    joined_data = pd.merge(left=dataset, 
                           right=measurement_stations,
                           left_on='NMT',
                           right_on='Nº',
                           how='inner').drop(columns=['NMT', 'Nº'])

    rename_cols = {
        'anio': 'year',
        'mes': 'month',
        'dia': 'day',
        'tipo': 'period',
        'LAEQ': 'avg_measure',
        'LAS01': 'p01',
        'LAS10': 'p10',
        'LAS50': 'p50',
        'LAS90': 'p90',
        'LAS99': 'p99',
        'LONGITUD_WGS84': 'longitude',
        'LATITUD_WGS84': 'latitude'
    }

    joined_data = joined_data.rename(columns=rename_cols)
    joined_data = joined_data[list(rename_cols.values())]

    convert_to_float = lambda x: float(x.replace(',', '.'))
    cols = ['avg_measure', 'p01', 'p10', 'p50', 'p90', 'p99']
    for c in cols:
        joined_data[c] = np.vectorize(convert_to_float)(joined_data[c])

    joined_data.to_csv(f'{data.CLEAN_DIR}noise_data.csv', index=False)

    #########################################################
    # Traffic data
    print('Traffic data')

    dataset = datasets[6]

    dataset['fecha'] = pd.to_datetime(dataset['fecha'])

    dataset['year'] = dataset['fecha'].dt.year
    dataset['month'] = dataset['fecha'].dt.month
    dataset['day'] = dataset['fecha'].dt.day
    dataset['hour'] = dataset['fecha'].dt.hour

    rename_cols = {
        'year': 'year',
        'month': 'month',
        'day': 'day',
        'hour': 'hour',
        'tipo_elem': 'measure_place',
        'intensidad': 'intensity',
        'ocupacion': 'occupation',
        'carga': 'congestion',
        'vmed': 'avg_speed'
    }

    dataset = dataset.drop(columns=['fecha', 'periodo_integracion']).rename(columns=rename_cols)
    dataset = dataset[list(rename_cols.values())]

    dataset.to_csv(f'{data.CLEAN_DIR}traffic_data.csv', index=False)




if __name__ == '__main__':
    main()