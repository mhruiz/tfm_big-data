import time
import random
import pandas as pd

colores = ['red', 'blue', 'green', 'pink']
getran = lambda x: x[random.randint(0, len(x)-1)]

coords = [
    [-3.6917268449043,40.4221320929972],
    [-3.69192878230734,40.4214333442836],
    [-3.69099128548591,40.4235122547331],
    [-3.69252547121172,40.4260039466159],
    [-3.69078016561876,40.4242200932479],
]

pd.DataFrame(coords, columns=['lon', 'lat']).to_csv('data.csv', index=False)

while True:
    df = pd.read_csv('data.csv')
    color = getran(colores)
    df['color'] = [color] * df.shape[0]
    df['percentage'] = [random.randint(0, 100) for _ in range(df.shape[0])]
    df.to_csv('data.csv', index=False)
    print('Current color:', color)
    time.sleep(5)
