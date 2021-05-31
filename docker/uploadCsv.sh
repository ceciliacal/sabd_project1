#!/bin/bash

sudo docker cp ./csv hdfs-namenode:/csv/
sudo docker exec -it hdfs-namenode hdfs dfs -put /cartella /cartella
