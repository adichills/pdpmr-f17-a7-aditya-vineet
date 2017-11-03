import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
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

    var loudnessCluster  = lines.map(x => (x(16),x(6).toDouble))
    val loudnessClusterCopy = loudnessCluster
    while(loudnessCluster.count() > 3) {
      val temp = agglomerative(sc,loudnessCluster)
      loudnessCluster = temp
    }
    val zippedCluster = loudnessCluster.zipWithIndex.map{case (k,v) => (v,k)}
    
    val loudnessCluster1 = zippedCluster.filter(x => x._1==0)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(loudnessClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    val loudnessCluster2 = zippedCluster.filter(x => x._1==1)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(loudnessClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
    
    val loudnessCluster3 = zippedCluster.filter(x => x._1==2)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(loudnessClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    loudnessCluster1.saveAsTextFile("AgglomerativeLoudnessCluster1")
    loudnessCluster2.saveAsTextFile("AgglomerativeLoudnessCluster2")
    loudnessCluster3.saveAsTextFile("AgglomerativeLoudnessCluster3")
    
    // 5 - length 6 - loudness 7 - tempo hottness - 25
    
    var lengthCluster  = lines.map(x => (x(16),x(5).toDouble))
    val lengthClusterCopy = lengthCluster
    while(lengthCluster.count() > 3) {
      val temp = agglomerative(sc,lengthCluster)
      lengthCluster = temp
    }
    val zippedCluster = lengthCluster.zipWithIndex.map{case (k,v) => (v,k)}
    
    val lengthCluster1 = zippedCluster.filter(x => x._1==0)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(lengthClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    val lengthCluster2 = zippedCluster.filter(x => x._1==1)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(lengthClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
    
    val lengthCluster3 = zippedCluster.filter(x => x._1==2)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(lengthClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    lengthCluster1.saveAsTextFile("AgglomerativeLengthCluster1")
    lengthCluster2.saveAsTextFile("AgglomerativeLengthCluster2")
    lengthCluster3.saveAsTextFile("AgglomerativeLengthCluster3")
    
    
    
    
    var tempoCluster  = lines.map(x => (x(16),x(7).toDouble))
    val tempoClusterCopy = tempoCluster
    while(tempoCluster.count() > 3) {
      val temp = agglomerative(sc,tempoCluster)
      tempoCluster = temp
    }
    val zippedCluster = tempoCluster.zipWithIndex.map{case (k,v) => (v,k)}
    
    val tempoCluster1 = zippedCluster.filter(x => x._1==0)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(tempoClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    val tempoCluster2 = zippedCluster.filter(x => x._1==1)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(tempoClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
    
    val tempoCluster3 = zippedCluster.filter(x => x._1==2)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(tempoClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    tempoCluster1.saveAsTextFile("AgglomerativeTempoCluster1")
    tempoCluster2.saveAsTextFile("AgglomerativeTempoCluster2")
    tempoCluster3.saveAsTextFile("AgglomerativeTempoCluster3")
    
    
    
    
    var hotnessCluster  = lines.map(x => (x(16),x(6).toDouble))
    val hotnessClusterCopy = hotnessCluster
    while(hotnessCluster.count() > 3) {
      val temp = agglomerative(sc,hotnessCluster)
      hotnessCluster = temp
    }
    val zippedCluster = hotnessCluster.zipWithIndex.map{case (k,v) => (v,k)}
    
    val hotnessCluster1 = zippedCluster.filter(x => x._1==0)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(hotnessClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    val hotnessCluster2 = zippedCluster.filter(x => x._1==1)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(hotnessClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
    
    val hotnessCluster3 = zippedCluster.filter(x => x._1==2)
                                .map{case(x,y) => (y)}
                                .flatMap(x => x._1.split(","))
                                .map(x => (x,1))
                                .join(hotnessClusterCopy)
                                .map{case(a,(b,c)) => (a,c)}
                                
    hotnessCluster1.saveAsTextFile("AgglomerativeHotnessCluster1")
    hotnessCluster2.saveAsTextFile("AgglomerativeHotnessCluster2")
    hotnessCluster3.saveAsTextFile("AgglomerativeHotnessCluster3")

  }


  def agglomerative(sc:SparkContext, rddInput:RDD[(String,Double)]): RDD[(String,Double)] ={
    val rdd1 = rddInput.sortBy(_._2)
    val first = rdd1.take(1)(0)
    val rdd2 = rdd1.filter(x => x!=first)

    val rdd1Indexed = rdd1.zipWithIndex.map{case (k,v) => (v,k)}
    val rdd2Indexed = rdd2.zipWithIndex.map{case (k,v) => (v,k)}
    val result = rdd1Indexed.join(rdd2Indexed).map{case(a,((b,c),(d,e))) => ((b,d),e-c)}.sortBy(_._2)

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
