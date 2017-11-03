import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Exception.allCatch


/*
	Author : Aditya Kammardi Sathyanarayan,Vineet

*/


object KmeansClustering {

  /*
	This method takes in an input string and returns true if the string can be converted to double else false
  */
  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  def getMinimumIndexEucledian(centroids: Array[(Double, Double)], metric: (Double, Double)): Int = {
    var min: Double = 10000000.0
    var minIndex: Int = 0
    for (i <- 0 to 2) {
      var diff = computeEuclideanDistance(centroids(i), metric)
      if (diff < min) {
        min = diff
        minIndex = i
      }
    }

    return minIndex
  }

  // This method takes in two 2D points and computes the euclidien distance between them
  def computeEuclideanDistance(point1: (Double, Double), point2: (Double, Double)): Double = {
    var result: Double = 0
    result = Math.sqrt(Math.pow(point1._1 - point2._1, 2) + Math.pow(point1._2 - point2._2, 2))
    return result
  }

  // This methods takes the 1D centroids and a 1D metric and returns the index of the centroid the metric is closest to
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


  // This method calculates a new 1D centroid from the input Frame of RDD[(String,Double,Int)]
  // Algo - Group all the songs by the centroid index they belong to and sum all the values in the cluster and find the average
  // the average is the new centroid point for that cluser
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

  // Works the same way as the above method ,but works on 2D data. The only problem is this is kind of inefficient ,as the
  // same computation is done twice once for the x co-ordinate and once for the y co-ordinate
  def updateCentroidEucledian(frame: RDD[(String, Double, Double, Int)], centroid: Array[(Double, Double)]): Array[(Double, Double)] = {
    val group1ByCluster = frame.map(row => (row._4, row._2))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .sortByKey()

    // {(0 , value1) ,(1,value 2),(2,value 3)}
    var cnt1 = group1ByCluster.take(3)


    val group2ByCluser = frame.map(row => (row._4, row._3))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .sortByKey()

    // {(0 , value1) ,(1,value 2),(2,value 3)}
    var cnt2 = group2ByCluser.take(3)


    var newCentroids: Array[(Double, Double)] = Array((cnt1(0)._2, cnt2(0)._2), (cnt1(1)._2, cnt2(1)._2), (cnt1(2)._2, cnt2(2)._2))
    return newCentroids
  }


  //This method assigns a new centroid to each song based on how close its metric is to the current 3 centroids
  def assignCentroids1D(frame: RDD[(String, Double, Int)], centroids: Array[Double]): RDD[(String, Double, Int)] = {
    println("In Assign Centroids")
    frame.map(row => (row._1, row._2, getMinimumIndex1D(centroids, row._2))).persist()

  }

  //This method assigns a new centroid to each song based on how close its metric is to the current 3 centroids in 2D
  def assignCentroids2D(frame: RDD[(String, Double, Double, Int)], centroids: Array[(Double, Double)]): RDD[(String, Double, Double, Int)] = {
    println("In Assign Centroids")
    frame.map(row => (row._1, row._2, row._3, getMinimumIndexEucledian(centroids, (row._2, row._3)))).persist()
  }


  //Performs K means for specified number of iterations on 1D data points
  // input - RDD consisting of SongId ,Metric ,CentroidIndex (initialized to -1),
  //          - Centroids (Randomly initialized)
  //          - iterations
  def kmeans1D(frame: RDD[(String, Double, Int)], centroids: Array[Double], iterations: Int): RDD[(String, Double, Int)] = {
    var f: RDD[(String, Double, Int)] = frame
    var c = centroids
    println("Calling kmeans")


    for (i <- 1 to iterations) {
      f = assignCentroids1D(f, c)

      c = updateCentroid1D(f, c)
      for (j <- 0 to 2) {
        println(centroids(j))
      }


    }
    return f
  }

  // Performs K means for specified number of iterations on 2D data points
  // input - RDD consisting of SongId ,Metric-x,Metric-y ,CentroidIndex (initialized to -1),
  //          - Centroids (Randomly initialized)
  //          - iterations
  def kmeans2D(frame: RDD[(String, Double, Double, Int)], centroids: Array[(Double, Double)], iterations: Int): RDD[(String, Double, Double, Int)] = {
    var f: RDD[(String, Double, Double, Int)] = frame
    var c = centroids
    println("Calling Kmeans")

    for (i <- 1 to iterations) {
      f = assignCentroids2D(f, c)

      c = updateCentroidEucledian(f, c)

      for (j <- 0 to 2) {
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

    //Turn of the logger
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("KmeansClustering")
    val sc = new SparkContext(conf)
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


    val t1 = System.currentTimeMillis()


    /*
      Important Assumption - We are considering the SongId field as the unique identifier of the songs as instead of trackid

     */

    /*
     check for corrupt numbers in loudness field and filter them out
     pick up songId , and loudness from each row

     assign each row 's cluster index as -1 initially

     Pick random centroids

     Call K-Means on the songId - Loudness values
   */
    val filteredLines1 = lines.filter(row => isDoubleNumber(row(6)))
    val loudnessValues = filteredLines1.map(row => (row(23), row(6).toDouble, -1))
    var centroids: Array[Double] = Array(0.0, -10.0, -30.0)
    var f1 = kmeans1D(loudnessValues, centroids, args(3).toInt)
    f1.map(row => row._1 + comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForLoudness")

    val t2 = System.currentTimeMillis()

    /*
    check for corrupt numbers in tempo field and filter them out
    pick up songId , and loudness from each row

    assign each row 's cluster index as -1 initially

    Pick random centroids

    Call K-Means on the songId - Tempo values
  */
    val filteredLines2 = lines.filter(row => isDoubleNumber(row(7)))
    val tempo = filteredLines2.map(row => (row(23), row(7).toDouble, -1))
    centroids = Array(0.0, 100.0, 200.0)
    var f2 = kmeans1D(tempo, centroids, args(3).toInt)
    f2.map(row => row._1 + comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForTempo")

    val t3 = System.currentTimeMillis()


    /*
    check for corrupt numbers in duration field and filter them out
    pick up songId , and duration from each row

    assign each row 's cluster index as -1 initially

    Pick random centroids

    Call K-Means on the songId - duration values
  */

    val filteredLines3 = lines.filter(row => isDoubleNumber(row(5)))
    val duration = filteredLines3.map(row => (row(23), row(5).toDouble, -1))
    centroids = Array(0.0, 1000.0, 1500.0)
    var f3 = kmeans1D(duration, centroids, args(3).toInt)
    f3.map(row => row._1 + comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForDuration")

    val t4 = System.currentTimeMillis()

    /*
    check for corrupt numbers in song hotness field and filter them out
    pick up songId , and Song hotness from each row

    assign each row 's cluster index as -1 initially

    Pick random centroids

    Call K-Means on the songId - songhotness values values
  */
    val filteredLines4 = lines.filter(row => isDoubleNumber(row(25)))
    val songhotness = filteredLines4.map(row => (row(23), row(25).toDouble, -1))
    centroids = Array(0.0, 0.3, 0.7)
    var f4 = kmeans1D(songhotness, centroids, args(3).toInt)
    f4.map(row => row._1 + comma + row._2 + comma + row._3).repartition(1).saveAsTextFile("outputForSonghotness")

    val t5 = System.currentTimeMillis()

    /*
    check for corrupt numbers in artist hotness fields and song hotness field and filter them out
    pick up songId ,artisthotness and Song hotness from each row

    assign each row 's cluster index as -1 initially

    Pick random centroids

    Call K-Means on the songId - songhotness values values
  */
    val filteredLines5 = lines.filter(row => isDoubleNumber(row(20)) && isDoubleNumber(row(25)))
    val songsByArtistHotnessAndSongHotness = filteredLines5.map(row => (row(23), row(20).toDouble, row(25).toDouble, -1))
    var centroids2d = Array((0.0, 0.0), (0.3, 0.3), (0.7, 0.7))
    var f5 = kmeans2D(songsByArtistHotnessAndSongHotness, centroids2d, args(3).toInt)
    f5.map(row => row._1 + comma + row._2 + comma + row._3 + comma + row._4).repartition(1).saveAsTextFile("outputForArtistHotnessAndSongHotness")

    val t6 = System.currentTimeMillis()
    println(("Loudness Query Executed in " + (t2 - t1) / 1000) + " seconds")
    println(("Tempo Query Executed in " + (t3 - t2) / 1000) + " seconds")
    println(("Duration Query Executed in " + (t4 - t3) / 1000) + " seconds")
    println(("Songhotness Query Executed in " + (t5 - t4) / 1000) + " seconds")
    println(("ArtistHotness and SongHotness Query Executed in " + (t6 - t5) / 1000) + " seconds")

  }


}
