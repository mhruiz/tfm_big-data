from .data import clean
import pandas as pd
import numpy as np


traffic_stations = None


def prepare_data(traffic_measures) -> pd.DataFrame:

    global traffic_stations

    if traffic_stations is None:
        traffic_stations = pd.read_csv('data/traffic_stations.csv')

    prepared_data = traffic_measures.merge(traffic_stations, on='id', how='left').drop(columns=['id'])

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