SPARK_HOME = /home/aditya/spark-2.2.0-bin-hadoop2.7
CLASSPATH=`echo $(SPARK_HOME)/jars/*.jar | tr ' ' :`
SONG_INFO = /home/aditya/songs/subset/song_info.csv
ARTIST_TERMS = /home/aditya/songs/subset/artist_terms.csv
SIMILAR_ARTISTS = /home/aditya/songs/subset/similar_artists.csv
OUTPUT_DIR = output
ITERATION = 10
all: build run



build: 
	scalac -cp $(CLASSPATH) scala/KmeansClustering.scala -d KmeansClustering.jar
	scalac -cp $(CLASSPATH) scala/Graph.scala -d Graph.jar
	scalac -cp $(CLASSPATH) scala/AgglomerativeClustering.scala -d Agglomerative.jar
	

run:
	$(SPARK_HOME)/bin/spark-submit --class KmeansClustering \
        --master local --deploy-mode client --executor-memory 2g \
        --name KmeansClustering --conf "spark.app.id=KmeansClustering" \
        KmeansClustering.jar $(SONG_INFO) $(ARTIST_TERMS) $(SIMILAR_ARTISTS) $(ITERATION)
	$(SPARK_HOME)/bin/spark-submit --class AgglomerativeClustering \
        --master local --deploy-mode client --executor-memory 2g \
        --name Agglomerative --conf "spark.app.id=Agglomerative" \
        Agglomerative.jar $(SONG_INFO) $(ARTIST_TERMS) $(SIMILAR_ARTISTS) $(ITERATION)
	$(SPARK_HOME)/bin/spark-submit --class Graph \
        --master local --deploy-mode client --executor-memory 2g \
        --name Graph --conf "spark.app.id=Graph" \
        Graph.jar $(SONG_INFO) $(ARTIST_TERMS) $(SIMILAR_ARTISTS) $(ITERATION)

