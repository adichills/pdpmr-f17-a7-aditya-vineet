import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.control.Exception.allCatch

object AgglomerativeClustering {
  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined



  def main(args: Array[String]) {

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




  }


  def agglomerative(): Unit ={
    val rdd1 = rddInput.sortBy(_._2)
    val first = rdd1.take(1)(0)
    val rdd2 = rdd1.filter(x => x!=first)

    val rdd1Indexed = rdd1.zipWithIndex.map{case (k,v) => (v,k)}
    val rdd2Indexed = rdd2.zipWithIndex.map{case (k,v) => (v,k)}
    val result = rdd1Indexed.join(rdd2Indexed).map{case(a,((b,c),(d,e))) => ((b,d),e.toDouble-c.toDouble)}.sortBy(_._2)

    val points = result.take(1)

    val name = points(0)._1._1 + "," + points(0)._1._2
    val point1 = rdd1.lookup(points(0)._1._1)
    val point2 = rdd1.lookup(points(0)._1._2)
    val point = (point1(0) + point2(0)) / 2
    val newPoint = (name, point)

    val filteredRdd = rdd1.filter(x => (x._1 != points(0)._1._1 && x._1 != points(0)._1._2))

    val newRdd = filteredRdd ++ sc.parallelize(List(newPoint))

    return newRdd

  }

  }
