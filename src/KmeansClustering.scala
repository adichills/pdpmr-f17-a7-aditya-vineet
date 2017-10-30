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

  def getMinimumIndex(centroids:Array[Double],metric:Double):Int={
    var min:Double = 100000000.0
    //println(centroids(1))
    var minIndex :Int = 0
    for( i <- 0 to 2 ){
	// println(centroids(i))
        // println(loudness)
         var diff = Math.abs(centroids(i)-metric)
        //println("diff is ***********************")
        //println(diff)
         if(diff< min){
           min = Math.abs(centroids(i)-metric)
           minIndex = i
         }
    }
//    if (minIndex == 0){
//      accumArray(0).add(metric)
//      accumArray(1).add(1)
//      //println(accumArray(0).value)
//    }
//    if (minIndex == 1){
//      accumArray(2).add(metric)
//      accumArray(3).add(1)
//    }
//    if (minIndex == 2){
//      accumArray(4).add(metric)
//      accumArray(5).add(1)
//    }

    return minIndex
  }

  def getMinimumIndices(centroidArray:Array[Array[Double]],metrics:Array[Double]): Array[Int] = {
    var mins:Array[Double] = Array(100000,100000,100000,100000)
    var minIndices : Array[Int] = Array(0,0,0,0)

    for( i <- 0 to centroidArray.length-1){
      for(j <- 0 to 2){
        var diff = Math.abs(centroidArray(i)(j) - metrics(i))
        if (diff < mins(i)){
          mins(i) = diff
          minIndices(i) = j
        }
      }
    }
    return minIndices
  }

  def updateCentroid(frame:RDD[(String,Double,Int)],c:Array[Double]):Array[Double] ={

//    val reduced = frame.map(row=>(row._2,row._3))
//
//
//    val quiet = reduced.filter(row => row._2 == 0).map(row=>row._1)
//    val medium = reduced.filter(row=>row._2 ==1).map(row=>row._1)
//    val loud = reduced.filter(row=>row._2==2).map(row=>row._1)



    val groupByCluser = frame
      .map(row=>(row._3,row._2))
      .mapValues(x=>(x,1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .sortByKey()

    var cnt = groupByCluser.take(3)
    for(i <- 0 to 2){
      c(i) = cnt(i)._2
    }


//    var c1 = quiet.sum/quiet.count
//    if(c1.isNaN){
//     c1 = c(0)
//   }
//    var c2 = medium.sum/medium.count
//    if(c2.isNaN){
//     c2 = c(1)
//   }
//
//   var c3 = loud.sum/loud.count
//   if(c3.isNaN){
//     c3 = c(2)
//   }
//
//
//    var centroids =  Array(c1,c2,c3)
    //return centroids
    return c
  }

  def updateCentroidReloaded(frame:RDD[(String,Array[Double],Array[Int])],cA:Array[Array[Double]]):Array[Array[Double]] ={
    for (i <- 0 to cA.length-1){
      val one = frame.filter(row => row._3(i) == 0).map(row => row._2(i))
      val two = frame.filter(row=> row._3(i)==1).map(row => row._2(i))
      val three = frame.filter(row=> row._3(i)==2).map(row => row._2(i))
      var c1 = one.sum/one.count
      if (c1.isNaN){
        c1 = cA(i)(0)
      }
      var c2 = two.sum/two.count
      if (c2.isNaN){
        c2 = cA(i)(1)
      }
      var c3 = three.sum/three.count
      if (c3.isNaN){
        c3 = cA(i)(2)
      }
      cA(i) = Array(c1,c2,c3)
    }
    return cA
  }





  def assignCentroids(frame:RDD[(String,Double, Int)],centroids:Array[Double]):RDD[(String,Double, Int)]= {
      println("In Assign Centroids")
//     for(m <- 0 to accumArray.length-1){
//      //println(accumArray(m).value)
//      accumArray(m).reset()
//      //println(accumArray(m).value)
//      }
      var f = frame.map(row => (row._1,row._2,getMinimumIndex(centroids,row._2)))
      return f
  }

  def assignCentroidsCombined(frame:RDD[(String,Array[Double],Array[Int])], centroidArray:Array[Array[Double]]):RDD[(String,Array[Double],Array[Int])]={
    println("In Assign Centroids")
    var f = frame.map(row=>(row._1,row._2,getMinimumIndices(centroidArray,row._2)))
    return f
  }

  def kmeans(frame : RDD [(String,Double, Int)],centroids:Array[Double]) : RDD[(String,Double,Int)] = {
    var f : RDD[(String,Double,Int)] = frame
    var c = centroids
    println("Calling kmeans")



   for(i <- 1 to 10){
      f = assignCentroids(f,c)
//      f.count()
//      centroids(0) = accumArray(0).value/accumArray(1).value
//      centroids(1) = accumArray(2).value/accumArray(3).value
//      centroids(2) = accumArray(4).value/accumArray(5).value

//      for(m <- 0 to accumArray.length-1){
//        println(accumArray(m))
//      }

      c = updateCentroid(f,c)
      for(j <- 0 to 2){
         println(centroids(j))
      }


    }
    return f
  }



  def kmeansCombined(frame:RDD[(String,Array[Double],Array[Int])]):RDD[(String,Array[Double],Array[Int])]={

    var f:RDD[(String,Array[Double],Array[Int])] = frame
    var centroidsArray = Array(Array(0.0,-10.0,-30.0),Array(0.0,100,200),Array(0.0,100,200),Array(0.0,0.3,0.7))
    println("Calling Kmeans")
    //for(i<-1 to 10){
      f = assignCentroidsCombined(f,centroidsArray)
      centroidsArray = updateCentroidReloaded(f,centroidsArray)
      //println("Iteration " + i)
      for(j<- 0 to centroidsArray.length-1){
        println("---------------------")
        println(j)
        for (k <- 0 to 2){
          println(centroidsArray(j)(k))
        }
      }
    //}

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


//    val sum1 = sc.doubleAccumulator("sum1")
//    val count1 = sc.doubleAccumulator("count1")
//
//    val sum2 = sc.doubleAccumulator("sum2")
//    val count2 = sc.doubleAccumulator("count2")
//
//    val sum3 = sc.doubleAccumulator("sum3")
//    val count3 = sc.doubleAccumulator("count3")


//    val accumArray : Array[DoubleAccumulator] = Array(sum1,count1,sum2,count2,sum3,count3)



    /*
      check for corrupt numbers in loudness field and filter them out
      pick up songId , song title and loudness from each row
      sortBy loudness and pick top 5
    */
    val flines = lines.filter(row => isDoubleNumber(row(6)))
    val loudnessValues = flines.map(row =>(row(23), row(6).toDouble,-1))
    var centroids:Array[Double]  = Array(0.0,-10.0,-30.0)
    var f1 = kmeans(loudnessValues,centroids)

    f1.saveAsTextFile("outputForLoudness")


    val flines2 = lines.filter(row => isDoubleNumber(row(7)))
    val temp = flines2.map(row =>(row(23), row(7).toDouble,-1))
    centroids = Array(0.0,100.0,200.0)
    var f2 = kmeans(temp,centroids)
    f2.saveAsTextFile("outputForTempo")

    val flines3 = lines.filter(row => isDoubleNumber(row(5)))
    val duration = flines3.map(row =>(row(23), row(5).toDouble,-1))
    centroids = Array(0.0,100.0,200.0)
    var f3 = kmeans(duration,centroids)
    f3.saveAsTextFile("outputForDuration")

    val flines4 = lines.filter(row => isDoubleNumber(row(25)))
    val songhotness = flines4.map(row =>(row(23), row(25).toDouble,-1))
    centroids = Array(0.0,0.3,0.7)
    var f4 = kmeans(songhotness,centroids)
    f4.saveAsTextFile("outputForSonghotness")




    //    val df = flines.map(row=> (row(23),Array(row(6).toDouble,row(7).toDouble,row(5).toDouble,row(25).toDouble),Array(-1,-1,-1,-1)))
//    var frame = kmeansReloaded(df)
//    val deflated = frame.map(row=> (row._1,row._2(0),row._3(0),row._2(1),row._3(1),row._2(2),row._3(2),row._2(3),row._3(3)))
//
//    //deflated.saveAsTextFile("outputReloaded")


  }
}
