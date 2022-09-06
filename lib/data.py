from io import BytesIO, StringIO
from typing import Generator, Iterable, Tuple, Union

import pandas as pd
import requests
import zipfile
import urllib
import os

from .utils import normalize_string, get_numeric_characters


API_URL = 'https://datos.madrid.es/egob/'
PATH = 'catalogo/title/'

API_FULL_PATH = API_URL + PATH

SAVE_DIR = 'data/raw/'

if not os.path.exists(SAVE_DIR):
    os.mkdir(SAVE_DIR)


def read_csvs_from_online_zip(zip_url: str) -> pd.DataFrame:
    '''
    Reads and concatenates all the CSV files inside a ZIP file.

    Parameters
    ----------
    zip_url : str
        URL to ZIP file.

    Returns
    -------
    pandas.Dataframe
        All CSV files concatenated into a single dataframe.
    '''

    zfile = zipfile.ZipFile(BytesIO(requests.get(zip_url).content))

    csv_files = list(filter(lambda x: 'csv' in x, zfile.namelist()))

    dataframes = [pd.read_csv(zfile.open(csv), sep=';') for csv in csv_files]

    return pd.concat(dataframes, axis=0)


def read_csv_from_url(csv_url: str) -> pd.DataFrame:
    '''
    Reads a CSV file from a website that does not allow "non-browser" requests.

    Parameters
    ----------
    csv_url : str
        URL to CSV file.

    Returns
    -------
    pandas.DataFrame
        CSV file loaded as a Dataframe.
    '''

    csv_content = requests.get(csv_url).text

    return pd.read_csv(StringIO(csv_content), sep=';')


def load_dataset(
    dataset_name: str, 
    format: str, 
    start_from: int, 
    verbose=False
) -> Union[Generator[pd.DataFrame, None, None], Tuple[pd.DataFrame, str]]:

    if verbose:
        print(f'Downloading {normalize_string(dataset_name)} dataset...')

    url_name = urllib.parse.quote(dataset_name)

    api_response = requests.get(API_FULL_PATH + url_name + '.json')

    assert api_response.ok, f'Something went wrong when downloading "{dataset_name}" dataset. API response code: {api_response.status_code}' 

    # just one response/dataset per dataset name -> get first item
    dataset_info = api_response.json()['result']['items'][0]['distribution']

    # iterate through the available files (if they are separated in years)
    try:
        available_files = [(file['accessURL'], \
                                file['format']['value'], \
                                    file['title']) for file in dataset_info \
                                                        if 'desde' in file['title'] or int(get_numeric_characters(file['title'])) >= start_from]
    except:
        # this dataset does not have yearly data
        available_files = [(file['accessURL'], \
                                file['format']['value'], \
                                    file['title']) for file in dataset_info]

    # if this dataset's files are given in ZIP format, extract their content and 
    # join it into a single Dataframe

    if format == 'zip':

        # ignore non-zip files
        available_files = list(filter(lambda x: x[1] == 'application/zip', available_files))

        available_files = list(map(lambda x: x[0], available_files))

        # set read content function
        read_content_fn = read_csvs_from_online_zip
            
    else:
        # read csv or txt files (if no csv is available)

        # get all csv files
        available_csvs = list(filter(lambda x: x[1] == 'text/csv', available_files))
        urls_csv, _, titles_csv = map(list, zip(*available_csvs))

        # get all txt files for those titles that does not have a csv file
        available_txts = list(filter(lambda x: x[2] not in titles_csv and x[1] == 'text/plain', available_files))
        if available_txts:
            urls_txt, _, _ = map(list, zip(*available_txts))
        else:
            urls_txt = []

        available_files = urls_csv + urls_txt

        # set read content function
        read_content_fn = read_csv_from_url

    if verbose:
        print(f'  Found {len(available_files)} files')

    # return file by file as Dataframes
    for file in available_files:
        df = read_content_fn(file)
        if verbose:
            print(f'    Dataframe size: {df.shape}')
        yield df



def save_dataset(dataframes: Union[Iterable[pd.DataFrame], pd.DataFrame], dataset_name: str):

    if type(dataframes) != pd.DataFrame:
        dataframes = pd.concat(dataframes, axis=0)

    dataframes.to_csv(f'{SAVE_DIR}{normalize_string(dataset_name)}.csv', index=False)