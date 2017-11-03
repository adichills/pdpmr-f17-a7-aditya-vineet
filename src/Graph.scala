import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Exception.allCatch


/*

    Author : Aditya and Vineet
 */

object Graph {

  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  def main(args:Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val songFileRDD = sc.textFile(args(0))
    val artistTermsRDD = sc.textFile(args(1))
    val simArtFileRDD = sc.textFile(args(2))

    // Process lines from songs_info.csv
    val songFileHeader = songFileRDD.first()
    val songs = songFileRDD.filter(row => row != songFileHeader)
    val lines = songs.map(rec => rec.split(";"))
    lines.persist()


    // process lines from similar artists
    val similarArtistsHeader = simArtFileRDD.first()
    val similarArtistsLines = simArtFileRDD.filter(row => row!=similarArtistsHeader)
    val artistSimilarity = similarArtistsLines.map(rec => rec.split(";"))
    artistSimilarity.persist()

    // process lines from artistTermsheader
    val artistTermHeader = artistTermsRDD.first()
    val artistTermLines = artistTermsRDD.filter(row => row!=artistTermHeader)
    val artistTerm = artistTermLines.map(rec => rec.split(";"))
    artistTerm.persist()


    val t1 = System.currentTimeMillis()
    // compute artist popularity

    val popularArtistsDescRDD = artistsByPopularity(sc,lines,artistSimilarity,artistTerm)

    popularArtistsDescRDD.take(30).foreach(x => println(x._1 + " " + x._2))
    val t2 = System.currentTimeMillis()


    // compute the commanlity graph
    val graph = commonality(sc,songs,artistSimilarity,artistTerm)
    var table2 = sc.parallelize(popularArtistsDescRDD.take(30)).map(x => x._1)
    val table1 = popularArtistsDescRDD.map(x => x._1)
    val t3 =  System.currentTimeMillis()
    for(i <- 1 to 10){
     // this is an expensive step (Need to do better than this)
     val start = table1.cartesian(table2).map(x => (x,1))
     val intermediate = start.join(graph)
                             .map{case ((a,b),(c,d)) => (a,(b,d)) }
                             .reduceByKey( (x,y) => if(x._2 > y._2) x else y )
                             .map{case (a,(b,c)) => (b,(a,c))}
                             .groupByKey()

     if(i == 10) {
       intermediate.repartition(1).saveAsTextFile("GraphClusters")
     }

     val newCentroid = intermediate.map{case (a,b) => (a,b.toList)}
                                   .map{case (a,b) => (a,b.sortBy(_._2))}
                                   .map{case(a,b) => (b(b.size/2))}
                                   .map(x => x._1)

     table2 = newCentroid
                                  
    }

    val t4 = System.currentTimeMillis()

    println(("Popularity Executed in " + (t2 - t1) / 1000) + " seconds")
    println(("Commanality graph Executed in " + (t3 - t2) / 1000) + " seconds")
    println(("K Means Clustering " + (t4 - t3) / 1000) + " seconds")


  }
  


  // This method generates the Commonailty graph edges
  // This method computes the artists popularity
  // Input : lines:RDD[Array[String]] lines from the song_info.csv without the header
  //
  //        artitstSimilarity:RDD[Array[String]] lines from the artist_similarity.csv without the header
  //        artistTerm :RDD[Array[String]] lines from the artistt terms file
  // output : RDD[(String,String) ,Int] is an RDD representing the graph's edges and its weight
  def commonality(sc:SparkContext,songs:RDD[String],artistSimilarity:RDD[Array[String]],artistTerm:RDD[Array[String]]): RDD[((String, String), Int)] = {

    val artistTermLines = artistTerm.map(row => (row(0), row(1))).groupByKey().map { case (a, b) => (a, b.toSet) }
    artistTermLines.persist()


    val similarArtLines = artistSimilarity.map(row => (row(0), row(1))).distinct()
    similarArtLines.persist()


    val artistSongs = songs.map(x => (x(16), 1)).groupByKey().map(x => (x._1, x._2.size))

    val similarArtists = similarArtLines.groupByKey().map(x => (x._1, x._2.size))

    // join similarArtists with artist terms
    // SimilarArtist(A1,A2) with ArtistTerm(A1,Terms) = >  Join(A1,(A2, Terms for A1))
    // Then flip the the keys A2 and A1 and join on artist terms again and perform an intersection of the terms for both artists
    //
    val joinedSimArtistTerms = similarArtLines.join(artistTermLines)

    val temp = joinedSimArtistTerms.map { case (a, (b, c)) => (b, (a, c)) }.join(artistTermLines)

    val graphEdges1 = temp.map { case (a, ((b, c), d)) => (a, b, c.intersect(d).size) }


    val graphEdges2 = graphEdges1.map { case (a, b, c) => (b, a, c) }

    // adding this to existing graph so that the undirected nature is maintained
    val graphEdgesUnion = graphEdges1 ++ graphEdges2

    return graphEdgesUnion.map{case(a,b,c) => ((a,b),c) }
  }

  // This method computes the artists popularity
  // Input : lines:RDD[Array[String]] lines from the song_info.csv without the header
  //
  //        artitstSimilarity:RDD[Array[String]] lines from the artist_similarity.csv without the header
  //        artistTerm :RDD[Array[String]] lines from the artistt terms file
  // Output : RDD[String,Double] which is popularity for each artist
  def artistsByPopularity(sc:SparkContext,lines:RDD[Array[String]],artistSimilarity:RDD[Array[String]],artistTerm:RDD[Array[String]]): RDD[(String, Double)] ={

    // check for sanity of artistFamiliarity
    val artistsByFamiliarity = lines.filter(row => isDoubleNumber(row(19)))
      .map(rec => (rec(16),rec(19).toDouble))
    // reason we take mean is because there are many a
    val meanArtistFamiliarity = artistsByFamiliarity.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)

    val artistSongs = lines.map(x => (x(16), 1)).groupByKey().map(x => (x._1,x._2.size))


    val similarArtists = artistSimilarity.map(x => (x(0), x(1))).groupByKey().map(x => (x._1,x._2.size))

    val popularity = artistSongs.join(similarArtists)
      .map(x => (x._1, (x._2._1) * (x._2._2)))
      .join(meanArtistFamiliarity)
      .map(x => (x._1, x._2._2 * x._2._1))
      .sortBy(_._2, false)

    return popularity
  }

}
