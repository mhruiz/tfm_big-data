import pandas as pd

from lib.data import raw


def main():

    for dataset_name, format, save_name, station_id in raw.RAW_DATA_NAMES[1:]:

        data_loader = raw.load_dataset(dataset_name, format, raw.START_YEAR, verbose=True)

        # get data from selected stations
        stations = pd.read_csv(f'{raw.SAVE_DIR}{save_name}_measuring_stations.csv')

        data = pd.concat(data_loader, axis=0)

        joined = pd.merge(data, stations, left_on=station_id, right_on='id', how='inner')

        raw.save_dataset(joined, save_name + '_data')
    
    ##############
    # traffic data
    dataset_name, format, save_name, station_id = raw.RAW_DATA_NAMES[0]

    # get data from selected stations
    stations = pd.read_csv(f'{raw.SAVE_DIR}{save_name}_measuring_stations.csv')

    traffic_dataloader = raw.load_dataset(dataset_name, format, raw.START_YEAR, verbose=True)

    dataframes = []

    for df in traffic_dataloader:  

        # only validated data and URB-only data
        df = df[(df['error'] == 'N') & (df['tipo_elem'] == 'URB')]

        # get data from selected stations
        df = pd.merge(df, stations, left_on=station_id, right_on='id', how='inner')

        df['fecha'] = pd.to_datetime(df['fecha']).dt.to_period('H')
        df_grb = df.groupby(['fecha', 'common_id']).mean().drop('id', axis=1).reset_index()
        # 12 measures for each hour (one per area)

        dataframes.append(df_grb)

    traffic_data = pd.concat(dataframes, axis=0).sort_values(by='fecha', ignore_index=True)

    raw.save_dataset(traffic_data, save_name + '_data')


if __name__ == '__main__':
    main()