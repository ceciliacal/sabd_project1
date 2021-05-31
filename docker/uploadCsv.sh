#!/bin/bash

CSV1="https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/punti-somministrazione-tipologia.csv"
CSV2="https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-latest.csv"
CSV3="https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-summary-latest.csv"

mkdir -m 777 data
cd ./data

wget $CSV1
wget $CSV2
wget $CSV3

cd ..

sudo docker cp data hdfs-namenode:/data/
sudo docker exec -it hdfs-namenode hdfs dfs -put /data /data
