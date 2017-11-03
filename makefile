SPARK_HOME = /home/aditya/spark-2.2.0-bin-hadoop2.7
CLASSPATH=`echo $(SPARK_HOME)/jars/*.jar | tr ' ' :`
SONG_INFO = /home/aditya/songs/subset/song_info.csv
ARTIST_TERMS = /home/aditya/songs/subset/artist_terms.csv
SIMILAR_ARTISTS = /home/aditya/songs/subset/similar_artists.csv
OUTPUT_DIR = output
all: build run



build: 
	#scalac -cp $(CLASSPATH) scala/KmeansClustering.scala -d KmeansClustering.jar
	scalac -cp $(CLASSPATH) scala/Graph.scala -d Graph.jar

run:
	
	$(SPARK_HOME)/bin/spark-submit --class Graph \
        --master local --deploy-mode client --executor-memory 2g \
        --name Graph --conf "spark.app.id=Graph" \
        Graph.jar $(SONG_INFO) $(ARTIST_TERMS) $(SIMILAR_ARTISTS)


