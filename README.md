# A7 - Clustering

### Given Million Song Database perform iterative computations, computations on a graph, learn basic clustering algorithms, using Apache Spark.

### Pre-requisite softwares:
***
* Scala
* Apache Spark 2.2
* R

### Edit - the below parameter's appropriately in the makefile
***
* SPARK_HOME
* SONG_INFO
* ARTIST_TERMS
* SIMILAR_ARTISTS
* SONG_INFO_GRAPH
* ARTIST_TERMS_GRAPH
* SIMILAR_ARTISTS_GRAPH
### Sample values are below
***
* SPARK_HOME = /home/ubuntu/spark-2.2.0-bin-hadoop2.7
* SONG_INFO = /home/ubuntu/all/song_info.csv
* ARTIST_TERMS = /home/ubuntu/all/artist_terms.csv
* OUTPUT_DIR = output

### The graph program runs only on subset dataset

#### substitute $SPARK_HOME appropriately as per your system

#### Run the project through terminal/linux shell by using the command "make".

### Below are the necessary steps before running make:
***
* Copy the songs_info.csv, artist_terms.csv, similar_artists.csv into the appropriate folders

### In order to generate report run the following command in R console:
***
* install.packages.("devtools")
* library(devtools)
* install_github("easyGgplot2","kassambara")

