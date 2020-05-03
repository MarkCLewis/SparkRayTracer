package srtrace

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._
import java.awt.image.BufferedImage
import javax.swing._
import java.awt.Graphics


object EricasTestingFile {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("ETF").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val partitions = 8

    val CartAndRadNumbers = Array[Int](5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011, 
    5012, 5013, 5014, 5015, 5016, 5017, 5018, 5019, 5020, 5021, 5022, 5023, 5024, 5025, 5026, 5027, 5028, 5029, 
    6000, 6001, 6002, 6003, 6004, 6005, 6006, 6007, 6008, 6009, 6010, 6011, 6012, 6013, 6014, 6015, 6016, 6017,
    6018, 6019, 6020, 6021, 6022, 6023, 6024, 6025,6026, 6027, 6028, 6029)

    def divisionOfFiles(p: Int, cAR: Array[Int]): RDD[(Int, Int)] = {
        val ret = Array.fill(cAR.length)((0,0))
        for (i <- cAR.indices) yield {
            ret(i) = (i % p, cAR(i))
        }
        sc.parallelize(ret)
    }

    println(divisionOfFiles(8, CartAndRadNumbers).collect().toList)


    //Need to make RDD[(Int, Int)] with (partition#, CartAndRad#)
    //Need to make RDD[(Int,(Int, Double, Double))] with p#, CAR#, offx, offy
    //Map to RDD[(Int, KDTreeGeometry)]

  }
}