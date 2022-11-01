
API_URL = 'https://datos.madrid.es/egob/'
PATH = 'catalogo/title/'

API_FULL_PATH = API_URL + PATH

TRAFFIC_STATIONS = 'Tráfico. Ubicación de los puntos de medida del tráfico'

MEASURING_STATIONS_DATA_NAMES = [
    TRAFFIC_STATIONS,
    'Calidad del aire. Estaciones de control',
    'Datos meteorológicos. Estaciones de control',
    'Contaminación acústica: Estaciones de medida'
]

RAW_DATA_NAMES = [
    ('Tráfico. Histórico de datos del tráfico desde 2013', 'zip', 'traffic',     'id'),
    ('Calidad del aire. Datos horarios desde 2001',        'zip', 'air_quality', 'ESTACION'),
    ('Datos meteorológicos. Datos horarios desde 2019',    'csv', 'weather',     'ESTACION'),
    ('Contaminación acústica. Datos diarios (acumulado)',  'csv', 'noise',       'NMT')
]

SAVE_DIR = 'data/raw/'

START_YEAR = 2019

CLEAN_DIR = 'data/clean/'

PROCESSED_DIR = 'data/processed/'
