import time
import random
import pandas as pd

import os

RANDOM_DATA_PATH = 'data.csv'

try:

    coords = [
        [-3.6917268449043,40.4221320929972],
        [-3.69192878230734,40.4214333442836],
        [-3.69099128548591,40.4235122547331],
        [-3.69252547121172,40.4260039466159],
        [-3.69078016561876,40.4242200932479],
    ]

    pd.DataFrame(coords, columns=['lon', 'lat']).to_csv(RANDOM_DATA_PATH, index=False)

    while True:
        df = pd.read_csv(RANDOM_DATA_PATH)
        df['percentage'] = [random.randint(0, 100) for _ in range(df.shape[0])]
        df.to_csv(RANDOM_DATA_PATH, index=False)
        print('Updated data')
        time.sleep(5)

except KeyboardInterrupt:
    os.remove(RANDOM_DATA_PATH)

finally:
    print('\nTerminated')