import lib.data.clean as clean
import lib.data.raw as raw
import lib.data as data

import pandas as pd
import numpy as np

import os


def main():

    # Load RAW csv files
    dataset_names = [x[2] for x in raw.RAW_DATA_NAMES]
    datasets = list(map(raw.read_dataset_from_raw, dataset_names))

    #########################################################
    # Air quality data
    print('Air quality data')

    hour_columns = r'H\d\d'
    valid_columns = r'V\d\d'

    data_melt = clean.multi_melt(datasets[1],
                                 [hour_columns, valid_columns],
                                 [('hour', 'measure'), ('valid', 'is_valid')])

    # validated data only
    data_melt = data_melt[data_melt['is_valid'] == 'V']

    # year,month,day,hour,daypart,magnitudes,common_id

    rename_cols = {
        'ANO': 'year',
        'MES': 'month',
        'DIA': 'day',
        'hour': 'hour',
        'MAGNITUD': 'magnitude',
        'measure': 'measure',
        'common_id': 'common_id'
    }

    delete_cols = [
        'valid',
        'PROVINCIA',
        'MUNICIPIO',
        'ESTACION',
        'PUNTO_MUESTREO',
        'valid',
        'is_valid',
        'id'
    ]

    data_melt = data_melt.drop(columns=delete_cols).rename(columns=rename_cols)
    data_melt = data_melt[list(rename_cols.values())]

    remove_H_prefix = lambda x: int(x[1:]) - 1
    data_melt['hour'] = np.vectorize(remove_H_prefix)(data_melt['hour'])

    # 0: D (diurno), 1: N (nocturno), 2: E (vespertino)
    set_daypart = lambda x: 0 if x == 'D' else 1 if x == 'N' else 2

    data_melt.insert(4, 'daypart', np.vectorize(set_daypart)(data_melt['hour']))

    air_magnitudes = {
        1: 'sulphur_dioxide',
        6: 'carbon_monoxide',
        7: 'nitrogen_monoxide',
        8: 'nitrogen_dioxide',
        9: 'particles_lt_2_5',
        10: 'particles_lt_10',
        12: 'nitrogen_oxides',
        14: 'ozone',
        20: 'toluene',
        30: 'benzene',
        35: 'ethylbenzene',
        37: 'metaxylene',
        38: 'paraxylene',
        39: 'ortoxylene',
        42: 'total_hydrocarbons',
        43: 'methane',
        44: 'non_methane_hydrocarbons'
    }

    data_melt['magnitude'] = np.vectorize(lambda x: air_magnitudes[x])(data_melt['magnitude'])

    data_melt_pivot = data_melt.pivot(columns='magnitude', values='measure', index=['year', 'month', 'day', 'hour', 'daypart', 'common_id'])
    data_melt_pivot = data_melt_pivot.reset_index()

    if not os.path.exists(data.CLEAN_DIR):
        os.mkdir(data.CLEAN_DIR)
    
    data_melt_pivot = clean.add_holiday_days(data_melt_pivot)

    data_melt_pivot.to_csv(f'{data.CLEAN_DIR}air_quality.csv', index=False)

    #########################################################
    # Meteorological data
    print('Meteorological data')

    dataset = datasets[2]

    # filter by year
    dataset = dataset[dataset['ANO'] >= raw.START_YEAR]

    data_melt = clean.multi_melt(dataset,
                            [hour_columns, valid_columns],
                            [('hour', 'measure'), ('valid', 'is_valid')])

    # validated data only
    data_melt = data_melt[data_melt['is_valid'] == 'V']

    # year,month,day,hour,daypart,magnitudes,common_id

    rename_cols = {
        'ANO': 'year',
        'MES': 'month',
        'DIA': 'day',
        'hour': 'hour',
        'MAGNITUD': 'magnitude',
        'measure': 'measure',
        'common_id': 'common_id'
    }

    delete_cols = [
        'valid',
        'PROVINCIA',
        'MUNICIPIO',
        'ESTACION',
        'PUNTO_MUESTREO',
        'valid',
        'is_valid',
        'id'
    ]

    data_melt = data_melt.drop(columns=delete_cols).rename(columns=rename_cols)
    data_melt = data_melt[list(rename_cols.values())]

    remove_H_prefix = lambda x: int(x[1:]) - 1
    data_melt['hour'] = np.vectorize(remove_H_prefix)(data_melt['hour'])

    # 0: D (diurno), 1: N (nocturno), 2: E (vespertino)
    set_daypart = lambda x: 0 if x == 'D' else 1 if x == 'N' else 2

    data_melt.insert(4, 'daypart', np.vectorize(set_daypart)(data_melt['hour']))

    weather_magnitudes = {
        80: 'uv_radiation',
        81: 'wind_speed',
        82: 'wind_direction',
        83: 'temperature',
        86: 'relative_humidity',
        87: 'bariometric_pressure',
        88: 'solar_radiation',
        89: 'precipitation'
    }

    data_melt['magnitude'] = np.vectorize(lambda x: weather_magnitudes[x])(data_melt['magnitude'])

    data_melt_pivot = data_melt.pivot(columns='magnitude', values='measure', index=['year', 'month', 'day', 'hour', 'daypart', 'common_id'])
    data_melt_pivot = data_melt_pivot.reset_index()

    data_melt_pivot = clean.add_holiday_days(data_melt_pivot)
    
    data_melt_pivot.to_csv(f'{data.CLEAN_DIR}meteorological_data.csv', index=False)

    #########################################################
    # Noise data
    print('Noise data')
    dataset = datasets[3]

    # filter by year
    dataset = dataset[dataset['anio'] >= raw.START_YEAR]

    rename_cols = {
        'anio': 'year',
        'mes': 'month',
        'dia': 'day',
        'tipo': 'daypart',
        'LAEQ': 'avg_noise',
        'LAS01': 'p01',
        'LAS10': 'p10',
        'LAS50': 'p50',
        'LAS90': 'p90',
        'LAS99': 'p99',
        'common_id': 'common_id'
    }

    dataset = dataset.rename(columns=rename_cols)
    dataset = dataset[list(rename_cols.values())]

    # 0: D (diurno), 1: N (nocturno), 2: E (vespertino)
    set_daypart = lambda x: 0 if x == 'D' else 1 if x == 'N' else 2
    dataset['daypart'] = np.vectorize(set_daypart)(dataset['daypart'])

    convert_to_float = lambda x: float(x.replace(',', '.'))
    cols = ['avg_noise', 'p01', 'p10', 'p50', 'p90', 'p99']
    for c in cols:
        dataset[c] = np.vectorize(convert_to_float)(dataset[c])

    # create 'hour' column by repeating values
    hours = {
        # diurno
        0: [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 18],
        # nocturno
        1: [23, 0, 1, 2, 3, 4, 5, 6],
        # vespertino
        2: [19, 20, 21, 22]
    }

    daypart_hour = [(dp, h) for dp in hours for h in hours[dp]]
    dayparts, hours = list(zip(*daypart_hour))

    daypart_hours = pd.DataFrame({
        'daypart': dayparts,
        'hour': hours
    })

    dataset = dataset.merge(daypart_hours, on='daypart', how='right')

    # year,month,day,hour,daypart,magnitudes,common_id
    dataset = dataset[['year', 'month', 'day', 'hour', 'daypart', 'common_id', 'avg_noise', 'p01', 'p10', 'p50', 'p90', 'p99']]

    dataset = clean.add_holiday_days(dataset)

    dataset.to_csv(f'{data.CLEAN_DIR}noise_data.csv', index=False)

    #########################################################
    # Traffic data
    print('Traffic data')

    dataset = datasets[0]

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
        'common_id': 'common_id',
        'intensidad': 'intensity',
        'ocupacion': 'occupation',
        'carga': 'congestion'
    }

    dataset = dataset.rename(columns=rename_cols)
    dataset = dataset[list(rename_cols.values())]

    dataset.insert(4, 'daypart', np.vectorize(set_daypart)(dataset['hour']))

    dataset = clean.add_holiday_days(dataset)

    dataset.to_csv(f'{data.CLEAN_DIR}traffic_data.csv', index=False)


if __name__ == '__main__':
    main()