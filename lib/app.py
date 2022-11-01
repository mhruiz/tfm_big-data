from typing import Union
from .data import clean, constants, raw
import pandas as pd
import numpy as np

from joblib import load

import datetime

traffic_stations = None


model = None
noise_min_max_values = None

noise_data = None


def prepare_data(traffic_measures) -> pd.DataFrame:

    global traffic_stations

    if traffic_stations is None:
        traffic_stations = pd.read_csv('data/traffic_stations.csv')

    prepared_data = traffic_measures.merge(traffic_stations, on='id', how='left').drop(columns=['id'])

    prepared_data = prepared_data[~prepared_data['longitud'].isna()]

    prepared_data['week'] = prepared_data['datetime'].dt.isocalendar().week

    hours = {
        # diurno
        0: [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
        # nocturno
        1: [23, 0, 1, 2, 3, 4, 5, 6],
        # vespertino
        2: [19, 20, 21, 22]
    }

    # 0: D (diurno), 1: N (nocturno), 2: E (vespertino)
    set_daypart = lambda x: next(dayp for dayp in hours if x in hours[dayp])

    prepared_data['year'] = prepared_data['datetime'].dt.year
    prepared_data['month'] = prepared_data['datetime'].dt.month
    prepared_data['day'] = prepared_data['datetime'].dt.day
    prepared_data['hour'] = prepared_data['datetime'].dt.hour

    prepared_data = prepared_data.drop(columns=['datetime'])

    prepared_data['daypart'] = np.vectorize(set_daypart)(prepared_data['hour'])

    prepared_data = clean.add_holiday_days(prepared_data)

    return prepared_data


def get_model_prediction(traffic_data: pd.DataFrame) -> pd.DataFrame:

    global model
    global noise_min_max_values

    if model is None:

        model = load(constants.MODEL_DIR + 'trained_model.joblib')

        noise_min_max_values = load(constants.MODEL_DIR + 'norm_values_output.joblib')

    predictions = model.predict(traffic_data[constants.INPUT_COLS])

    # convert values from [0, 1]
    for i, c in enumerate(constants.OUTPUT_COLS):

        min_ = noise_min_max_values[c][0]
        max_ = noise_min_max_values[c][1]

        predictions[:, i] = (max_ - min_) * predictions[:, i] + min_

    predictions = pd.DataFrame(predictions, columns=constants.OUTPUT_COLS)

    return pd.concat([traffic_data[['longitud', 'latitud']], predictions], axis=1)


def update_data(last_sync: datetime.datetime = None) -> Union[pd.DataFrame, datetime.datetime]:

    global noise_data

    traffic_data = next(raw.load_dataset(constants.REAL_TIME_TRAFFIC_DATA, 'xml', 2022, verbose=False))

    data_sync = traffic_data['datetime'].max().to_pydatetime()

    if last_sync is None or data_sync != last_sync:

        prepared_data = prepare_data(traffic_data)

        noise_data = get_model_prediction(prepared_data).rename(columns={'avg_noise': 'Ruido medio (dB)'})

    return noise_data, data_sync
