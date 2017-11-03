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

  def getMinimumIndex(centroids: Array[Double], metric: Double): Int = {
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


  def updateCentroid(frame: RDD[(String, Double, Int)], c: Array[Double]): Array[Double] = {

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


  def assignCentroids(frame: RDD[(String, Double, Int)], centroids: Array[Double]): RDD[(String, Double, Int)] = {
    println("In Assign Centroids")
    frame.map(row => (row._1, row._2, getMinimumIndex(centroids, row._2))).persist()
    //return f
  }


  def kmeans(frame: RDD[(String, Double, Int)], centroids: Array[Double]): RDD[(String, Double, Int)] = {
    var f: RDD[(String, Double, Int)] = frame
    var c = centroids
    println("Calling kmeans")


    for (i <- 1 to 10) {
      f = assignCentroids(f, c)

      c = updateCentroid(f, c)
      for (j <- 0 to 2) {
        println(centroids(j))
      }


    }
    return f
  }


  def main(args: Array[String]) {

    /*
      Initialize spark context

    */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("KmeansClustering")
    val sc = new SparkContext(conf)

    /*
      List buffer to hold the output data
    */
    var output = new ListBuffer[String]()

    val t1 = System.currentTimeMillis()

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
    val flines = lines.filter(row => isDoubleNumber(row(6)))
    val loudnessValues = flines.map(row => (row(23), row(6).toDouble, -1))
    var centroids: Array[Double] = Array(0.0, -10.0, -30.0)
    var f1 = kmeans(loudnessValues, centroids)

    val t2 = System.currentTimeMillis();

    println((t2 - t1) / 1000)


    val flines2 = lines.filter(row => isDoubleNumber(row(7)))
    val temp = flines2.map(row => (row(23), row(7).toDouble, -1))
    centroids = Array(0.0, 100.0, 200.0)
    var f2 = kmeans(temp, centroids)
    f2.saveAsTextFile("outputForTempo")

    val flines3 = lines.filter(row => isDoubleNumber(row(5)))
    val duration = flines3.map(row => (row(23), row(5).toDouble, -1))
    centroids = Array(0.0, 100.0, 200.0)
    var f3 = kmeans(duration, centroids)
    f3.saveAsTextFile("outputForDuration")

    val flines4 = lines.filter(row => isDoubleNumber(row(25)))
    val songhotness = flines4.map(row => (row(23), row(25).toDouble, -1))
    centroids = Array(0.0, 0.3, 0.7)
    var f4 = kmeans(songhotness, centroids)
    f4.saveAsTextFile("outputForSonghotness")


  }

  def test(args: RDD[(String, (String, Iterable[String]))]) = {

  }

  def commanlity(artFile: String, simArtFile: String, songFile: String, sc: SparkContext) = {
    val artistTermsRDD = sc.textFile(artFile)
    val simArtFileRDD = sc.textFile(simArtFile)

    val header1 = artistTermsRDD.first()
    val header2 = simArtFileRDD.first()

    val artistTermLines = artistTermsRDD.filter(row => row != header1).map(row => row.split(";")).map(row => (row(0), row(1))).groupByKey().map { case (a, b) => (a, b.toSet) }
    artistTermLines.persist()

    val songsFile = sc.textFile(songsFile)
    val songsHeader = songsFile.first()
    val songLines = songsFile.filter(row => row != songsHeader)
    val songs = songLines.map(rec => rec.split(";"))
    songs.persist()


    val similarArtLines = simArtFileRDD.filter(row => row != header2).map(row => row.split(";")).map(row => (row(0), row(1))).distinct()
    similarArtLines.persist()

    val artistsByFamiliarity = songs.filter(row => isDoubleNumber(row(19)))
      .map(rec => (rec(16), rec(19).toDouble))
    val meanArtistFamiliarity = artistsByFamiliarity.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)

    val artistSongs = songs.map(x => (x(16), 1)).groupByKey().map(x => (x._1, x._2.size))

    val similarArtists = similarArtLines.groupByKey().map(x => (x._1, x._2.size))


    val joinedSimArtistTerms = similarArtLines.join(artistTermLines)

    val temp = joinedSimArtistTerms.map { case (a, (b, c)) => (b, (a, c)) }.join(artistTermLines)

    val graphEdges1 = temp.map { case (a, ((b, c), d)) => (a, b, c.intersect(d).size) }


    val graphEdges2 = graphEdges1.map { case (a, b, c) => (b, a, c) }

    val graphEdgesUnion = graphEdges1 ++ graphEdges2


  }


  def trendSetters(sc: SparkContext, songs: RDD[Array[String]], artistSimilarity: RDD[Array[String]], artistTerms: RDD[Array[String]])
  : RDD[(String, Double)] = {

    val artistsByFamiliarity = songs.filter(row => isDoubleNumber(row(19)))
      .map(rec => (rec(16), rec(19).toDouble))
    val meanArtistFamiliarity = artistsByFamiliarity.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)

    val artistSongs = songs.map(x => (x(16), 1)).groupByKey().map(x => (x._1, x._2.size))

    val similarArtists = artistSimilarity.map(x => (x(0), x(1))).groupByKey().map(x => (x._1, x._2.size))


    val popularity = artistSongs.join(similarArtists)
      .map(x => (x._1, (x._2._1) * (x._2._2)))
      .join(meanArtistFamiliarity)
      .map(x => (x._1, x._2._2 * x._2._1))
      .sortBy(_._2, false)
    return popularity

  }


}
