from typing import List, Tuple
import pandas as pd
import numpy as np
import re, os

from .raw import load_dataset, START_YEAR

HOLIDAYS_PATH = 'data/clean/holidays.csv'
holidays_data = None

def multi_melt(df: pd.DataFrame, regexr: List[str], new_names: List[Tuple[str,str]]):

    columns = df.columns.values

    column_matches = np.full(columns.shape[0], False, dtype=bool)

    regex_matches = []

    for regex in regexr:
        matches = np.vectorize(lambda x: len(re.findall(regex, x)) > 0)(df.columns)

        column_matches = column_matches | matches
        regex_matches.append(columns[matches].tolist())

    base_columns = columns[~column_matches].tolist()
    melted_dfs = []

    for matches, names in zip(regex_matches, new_names):
        melted_dfs.append(df[base_columns + matches].melt(id_vars=base_columns, var_name=names[0], value_name=names[1]))
        base_columns = []

    return pd.concat(melted_dfs, axis=1)


def load_holidays_data() -> pd.DataFrame:

    global holidays_data

    if holidays_data is None:
        if os.path.exists(HOLIDAYS_PATH):
            holidays_data = pd.read_csv(HOLIDAYS_PATH)
            return holidays_data

    # Holidays calendar
    holidays_data = next(load_dataset('Calendario laboral', 'csv', start_from=START_YEAR, verbose=True))

    holidays_data.columns = ['day', 'week_day', 'day_type', 'holiday_type', 'holiday']

    holidays_data = holidays_data[['day', 'week_day', 'day_type']]

    holidays_data['week_day'] = holidays_data['week_day'].str.lower()
    holidays_data['day_type'] = holidays_data['day_type'].str.lower()

    holidays_data['day_type'] = np.vectorize(lambda x: x if x != 'sã¡bado' else 'sabado')(holidays_data['day_type'])

    week_days = {
        'sã¡bado': 'sabado',
        'miã©rcoles': 'miercoles'
    }
    holidays_data['week_day'] = np.vectorize(lambda x: x if week_days.get(x) is None else week_days[x])(holidays_data['week_day'])

    holidays_data = holidays_data[~holidays_data['day'].isna()]

    holidays_data = holidays_data[(holidays_data['day_type'] != 'laborable') & (holidays_data['day_type'] != 'nan') | 
                                  (holidays_data['day_type'] == 'nan')       & (holidays_data['week_day'].isin(['sabado', 'domingo']))]

    holidays_data['day'] = pd.to_datetime(holidays_data['day'], format='%d/%m/%Y')
    holidays_data['year'] = holidays_data['day'].dt.year
    holidays_data['month'] = holidays_data['day'].dt.month
    holidays_data['day'] = holidays_data['day'].dt.day

    holidays_data = holidays_data[['year', 'month', 'day', 'week_day']]

    holidays_data = holidays_data[holidays_data['year'] >= 2019].sort_values(by=['year', 'month', 'day']).reset_index(drop=True)
    
    holidays_data.to_csv(HOLIDAYS_PATH, index=False)

    return holidays_data


def add_holiday_days(df: pd.DataFrame) -> pd.DataFrame:

    global holidays_data

    if holidays_data is None:
        load_holidays_data()

    df_new = df.merge(holidays_data, on=['year', 'month', 'day'], how='left')

    df_new.insert(5, 'is_holiday', np.vectorize(lambda x: 0 if x != x else 1)(df_new['week_day']))

    df_new = df_new.drop(columns='week_day')

    return df_new
