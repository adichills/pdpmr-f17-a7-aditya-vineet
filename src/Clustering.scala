import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Exception.allCatch
import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level



/*
	Author : Aditya Kammardi Sathyanarayan, Vineet Trivedi

*/



object Clustering {

  /*
	This method takes in an input string and returns true if the string can be converted to double else false
  */
  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  def getMinimumIndex(centroids:Array[Double],loudness:Double):Int={
    var min:Double = 100000000.0
    //println(centroids(1))
    var minIndex :Int = 0
    for( i <- 0 to 2 ){
	// println(centroids(i))
        // println(loudness)
         var diff = Math.abs(centroids(i)-loudness)
        // println("diff is ***********************")
        // println(diff)
         if(diff< min){
           min = Math.abs(centroids(i)-loudness)
           minIndex = i
         }
    }
    return minIndex
  }

  def updateCentroid(frame:RDD[(String,Double,Int)],c:Array[Double]):Array[Double] ={

    val reduced = frame.map(row=>(row._2,row._3))
    val quiet = reduced.filter(row => row._2 == 0).map(row=>row._1)
    val medium = reduced.filter(row=>row._2 ==1).map(row=>row._1)
    val loud = reduced.filter(row=>row._2==2).map(row=>row._1)
    var c1 = quiet.sum/quiet.count
    if(c1.isNaN){
     c1 = c(0)
   }
    var c2 = medium.sum/medium.count
    if(c2.isNaN){
     c2 = c(1)
   }
   
   var c3 = loud.sum/loud.count
   if(c3.isNaN){
     c3 = c(2)
   }


    var centroids =  Array(c1,c2,c3)
    return centroids

  }

  def assignCentroidsManhatten(frame:RDD[(String,Double, Int)], centroids:Array[Double]):RDD[(String,Double, Int)]= {
      println("In Assign Centroids")
      //println(centroids(1))
      var f = frame.map(row => (row._1,row._2,getMinimumIndex(centroids,row._2)))
      return f
  }

  def kmeansManhatten(frame : RDD [(String,Double, Int)]) : RDD[(String,Double,Int)] = {
    var f : RDD[(String,Double,Int)] = frame
    var centroids:Array[Double]  = Array(0.0,200.0,400.0)
    println("Calling kmeans")
   for(i <- 1 to 10){
      f = assignCentroidsManhatten(frame,centroids)
      centroids = updateCentroid(f,centroids)
      for(j <- 0 to 2){
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
    val conf = new SparkConf().setAppName("Clustering")
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
    val songs = songFile.filter(row => row!=header)
    val lines = songs.map(rec => rec.split(";"))
    lines.persist()





    /*
      check for corrupt numbers in loudness field and filter them out
      pick up songId , song title and loudness from each row
      sortBy loudness and pick top 5
    */
    val flines = lines.filter(row => isDoubleNumber(row(7)))
    val loudnessValues = flines.map(row =>(row(23), row(7).toDouble,-1))
    var frame = kmeansManhatten(loudnessValues)
    frame.saveAsTextFile("outputDuration")


  }
}
