sudo docker exec spark /bin/bash -c "spark-submit --class queries.StartQueries --master \"spark://localhost:7077\" /queries/sabd_project1-1.0-SNAPSHOT.jar"
