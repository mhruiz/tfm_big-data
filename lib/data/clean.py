from typing import List, Tuple
import pandas as pd
import numpy as np
import re


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


