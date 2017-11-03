import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.DoubleAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.control.Exception.allCatch


/*
	Author : Aditya Kammardi Sathyanarayan, Vineet Trivedi

*/


object KmeansClustering {

  /*
	This method takes in an input string and returns true if the string can be converted to double else false
  */
  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  def getMinimumIndexEucledian(centroids:Array[(Double,Double)],metric:(Double,Double)):Int = {
    var min:Double = 10000000.0
    var minIndex:Int = 0
    for(i <-0 to 2){
      var diff = computeEuclideanDistance(centroids(i),metric)
      if(diff< min){
        min = diff
        minIndex = i
      }
    }

    return minIndex
  }

  def computeEuclideanDistance(point1:(Double,Double),point2:(Double,Double)) :Double = {
    var result:Double = 0
    result = Math.sqrt(Math.pow(point1._1-point2._1,2) + Math.pow(point1._2-point2._2,2))
    return result
  }

  def getMinimumIndex1D(centroids: Array[Double], metric: Double): Int = {
    var min: Double = 100000000.0
    var minIndex: Int = 0
    for (i <- 0 to 2) {
      var diff = Math.abs(centroids(i) - metric)
      if (diff < min) {
        min = Math.abs(centroids(i) - metric)
        minIndex = i
      }
    }

    return minIndex
  }




  def updateCentroid1D(frame: RDD[(String, Double, Int)], c: Array[Double]): Array[Double] = {

    val groupByCluser = frame
      .map(row => (row._3, row._2))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .sortByKey()

    var cnt = groupByCluser.take(3)
    for (i <- 0 to 2) {
      c(i) = cnt(i)._2
    }

    return c
  }

  def updateCentroidEucledian(frame:RDD[(String,Double,Double,Int)],centroid:Array[(Double,Double)]):Array[(Double,Double)] = {
    val group1ByCluster  = frame.map(row=> (row._4,row._2))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .sortByKey()

    var cnt1 = group1ByCluster.take(3)


    val group2ByCluser = frame.map(row=> (row._4,row._3))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .sortByKey()


    var cnt2 = group2ByCluser.take(3)


    var newCentroids : Array[(Double,Double)] = Array((cnt1(0)._2,cnt2(0)._2),(cnt1(1)._2,cnt2(1)._2),(cnt1(2)._2,cnt2(2)._2))
    return newCentroids
  }


  def assignCentroids1D(frame: RDD[(String, Double, Int)], centroids: Array[Double]): RDD[(String, Double, Int)] = {
    println("In Assign Centroids")
    frame.map(row => (row._1, row._2, getMinimumIndex1D(centroids, row._2))).persist()
    //return f
  }

  def assignCentroids2D(frame:RDD[(String,Double,Double,Int)],centroids:Array[(Double,Double)]) : RDD[(String,Double,Double,Int)] = {
    println("In Assign Centroids")
    frame.map(row => (row._1,row._2,row._3,getMinimumIndexEucledian(centroids,(row._2,row._3)))).persist()
  }


  def kmeans1D(frame: RDD[(String, Double, Int)], centroids: Array[Double],iterations:Int): RDD[(String, Double, Int)] = {
    var f: RDD[(String, Double, Int)] = frame
    var c = centroids
    println("Calling kmeans")


    for (i <- 1 to 10) {
      f = assignCentroids1D(f, c)

      c = updateCentroid1D(f, c)
      for (j <- 0 to 2) {
        println(centroids(j))
      }


    }
    return f
  }

  def kmeans2D(frame:RDD[(String,Double,Double,Int)],centroids:Array[(Double,Double)],iterations:Int) : RDD[(String,Double,Double,Int)] = {
    var f : RDD[(String,Double,Double,Int)] = frame
    var c = centroids
    println("Calling Kmeans")

    for(i <- 1 to iterations){
      f = assignCentroids2D(f,c)

      c = updateCentroidEucledian(f,c)

      for(j<-0 to 2){
        println(c(j))
      }
    }

    return f
  }


  def main(args: Array[String]) {

    /*
      Initialize spark context

    */

    val comma = ","

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("KmeansClustering")
    val sc = new SparkContext(conf)

    /*
      List buffer to hold the output data
    */
    var output = new ListBuffer[String]()



    /*
      Load the contents song file into a val
    */
    val songFile = sc.textFile(args(0))
    val header = songFile.first()
    /*
      Remove the first row and split each row by ';' and make the resulting rdd persist
    */
    val songs = songFile.filter(row => row != header)
    val lines = songs.map(rec => rec.split(";"))
    lines.persist()


    /*
      check for corrupt numbers in loudness field and filter them out
      pick up songId , song title and loudness from each row
      sortBy loudness and pick top 5
    */
    val t1 = System.currentTimeMillis()
//    val flines = lines.filter(row => isDoubleNumber(row(6)))
//    val loudnessValues = flines.map(row => (row(23), row(6).toDouble, -1))
//    var centroids: Array[Double] = Array(0.0, -10.0, -30.0)
//    var f1 = kmeans1D(loudnessValues, centroids,args(3).toInt)
//    f1.map(row => row._1+ comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForLoudness")

    val t2 = System.currentTimeMillis()


//    val flines2 = lines.filter(row => isDoubleNumber(row(7)))
//    val temp = flines2.map(row => (row(23), row(7).toDouble, -1))
//    centroids = Array(0.0, 100.0, 200.0)
//    var f2 = kmeans1D(temp, centroids,args(3).toInt)
//    f2.map(row => row._1+ comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForTempo")

    val t3 = System.currentTimeMillis()

    val flines3 = lines.filter(row => isDoubleNumber(row(5)))
    val duration = flines3.map(row => (row(23), row(5).toDouble, -1))
    var  centroids = Array(0.0, 1000.0, 1500.0)
    var f3 = kmeans1D(duration, centroids,args(3).toInt)
    f3.map(row => row._1+ comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForDuration")

    val t4 = System.currentTimeMillis()
//
//    val flines4 = lines.filter(row => isDoubleNumber(row(25)))
//    val songhotness = flines4.map(row => (row(23), row(25).toDouble, -1))
//    centroids = Array(0.0, 0.3, 0.7)
//    var f4 = kmeans1D(songhotness, centroids,args(3).toInt)
//    f4.map(row => row._1+ comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForSonghotness")

    val t5 = System.currentTimeMillis()

//    val flines5 = lines.filter(row => isDoubleNumber(row(20)) && isDoubleNumber(row(25)))
//    val songsByArtistHotnessAndSongHotness = flines5.map(row => (row(23),row(20).toDouble,row(25).toDouble,-1))
//    var centroids2d = Array((0.0,0.0),(0.3,0.3),(0.7,0.7))
//    var f5 = kmeans2D(songsByArtistHotnessAndSongHotness,centroids2d,args(3).toInt)
//    f5.map(row => row._1+ comma + row._2 + comma + row._3 + comma + row._4).repartition(1).saveAsTextFile("outputForArtistHotnessAndSongHotness")

    val t6 = System.currentTimeMillis()
    println(("Loudness Query Executed in "+ (t2 - t1) / 1000) + " seconds")
    println(("Tempo Query Executed in "+ (t3 - t2) / 1000) + " seconds")
    println(("Duration Query Executed in "+ (t4 - t3) / 1000) + " seconds")
    println(("Songhotness Query Executed in "+ (t5 - t4) / 1000) + " seconds")
    println(("ArtistHotness and SongHotness Query Executed in "+ (t6 - t5) / 1000) + " seconds")

  }



//  def commanlity(artFile: String, simArtFile: String, songFile: String, sc: SparkContext) = {
//    val artistTermsRDD = sc.textFile(artFile)
//    val simArtFileRDD = sc.textFile(simArtFile)
//
//    val header1 = artistTermsRDD.first()
//    val header2 = simArtFileRDD.first()
//
//    val artistTermLines = artistTermsRDD.filter(row => row != header1).map(row => row.split(";")).map(row => (row(0), row(1))).groupByKey().map { case (a, b) => (a, b.toSet) }
//    artistTermLines.persist()
//
//    val songsFile = sc.textFile(songsFile)
//    val songsHeader = songsFile.first()
//    val songLines = songsFile.filter(row => row != songsHeader)
//    val songs = songLines.map(rec => rec.split(";"))
//    songs.persist()
//
//
//    val similarArtLines = simArtFileRDD.filter(row => row != header2).map(row => row.split(";")).map(row => (row(0), row(1))).distinct()
//    similarArtLines.persist()
//
//    val artistsByFamiliarity = songs.filter(row => isDoubleNumber(row(19)))
//      .map(rec => (rec(16), rec(19).toDouble))
//    val meanArtistFamiliarity = artistsByFamiliarity.mapValues(x => (x, 1))
//      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//      .mapValues(y => 1.0 * y._1 / y._2)
//
//    val artistSongs = songs.map(x => (x(16), 1)).groupByKey().map(x => (x._1, x._2.size))
//
//    val similarArtists = similarArtLines.groupByKey().map(x => (x._1, x._2.size))
//
//
//    val joinedSimArtistTerms = similarArtLines.join(artistTermLines)
//
//    val temp = joinedSimArtistTerms.map { case (a, (b, c)) => (b, (a, c)) }.join(artistTermLines)
//
//    val graphEdges1 = temp.map { case (a, ((b, c), d)) => (a, b, c.intersect(d).size) }
//
//
//    val graphEdges2 = graphEdges1.map { case (a, b, c) => (b, a, c) }
//
//    val graphEdgesUnion = graphEdges1 ++ graphEdges2
//
//
//  }
//
//
//  def trendSetters(sc: SparkContext, songs: RDD[Array[String]], artistSimilarity: RDD[Array[String]], artistTerms: RDD[Array[String]])
//  : RDD[(String, Double)] = {
//
//    val artistsByFamiliarity = songs.filter(row => isDoubleNumber(row(19)))
//      .map(rec => (rec(16), rec(19).toDouble))
//    val meanArtistFamiliarity = artistsByFamiliarity.mapValues(x => (x, 1))
//      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//      .mapValues(y => 1.0 * y._1 / y._2)
//
//    val artistSongs = songs.map(x => (x(16), 1)).groupByKey().map(x => (x._1, x._2.size))
//
//    val similarArtists = artistSimilarity.map(x => (x(0), x(1))).groupByKey().map(x => (x._1, x._2.size))
//
//
//    val popularity = artistSongs.join(similarArtists)
//      .map(x => (x._1, (x._2._1) * (x._2._2)))
//      .join(meanArtistFamiliarity)
//      .map(x => (x._1, x._2._2 * x._2._1))
//      .sortBy(_._2, false)
//    return popularity
//
//  }


}
