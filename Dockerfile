FROM python:3.9

ADD py_web_app.py .
ADD lib ./lib
ADD data/traffic_stations.csv ./data/traffic_stations.csv

RUN mkdir -p ./data/clean

ADD model ./model
ADD requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 8050

CMD python ./py_web_app.py