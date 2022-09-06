import pandas as pd

from lib import utils, data


START_YEAR = 2019


def main():

    dataset_names = pd.read_csv('./data/datasets_madrid.csv')

    traffic_dataset = dataset_names.tail(1).values[0]

    dataset_names = dataset_names.iloc[:-1]

    for _, name_fmt in dataset_names.iterrows():

        name = name_fmt['Dataset name']
        fmt = name_fmt['Format']

        data_loader = data.load_dataset(name, fmt, START_YEAR, verbose=True)

        data.save_dataset(data_loader, name)

    # traffic data
    traffic_dataloader = data.load_dataset(traffic_dataset[0], traffic_dataset[1], START_YEAR, verbose=True)

    dataframes = []

    for df in traffic_dataloader:  

        # only validated data
        df = df[df['error'] == 'N']

        df['fecha'] = pd.to_datetime(df['fecha']).dt.to_period('H')

        df_grb = df.groupby(['fecha', 'tipo_elem']).mean().drop('id', axis=1).reset_index()

        dataframes.append(df_grb)

    traffic_data = pd.concat(dataframes, axis=0).sort_values(by='fecha', ignore_index=True)

    data.save_dataset(traffic_data, traffic_dataset[0])


if __name__ == '__main__':
    main()