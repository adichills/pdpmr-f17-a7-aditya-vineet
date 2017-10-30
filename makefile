
SPARK_HOME = /home/aditya/spark-2.2.0-bin-hadoop2.7
SONG_INFO = /home/aditya/songs/all/song_info.csv
ARTIST_TERMS = /home/ubuntu/all/artist_terms.csv
OUTPUT_DIR = output
all: build run



build: 
	scalac scala/Clustering.scala -d Clustering.jar

run:
	$(SPARK_HOME)/bin/spark-submit --class Clustering \
	--master local --deploy-mode client --executor-memory 2g \
	--name Clustering --conf "spark.app.id=Clustering" \
	Clustering.jar $(SONG_INFO)


