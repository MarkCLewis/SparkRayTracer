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
    val conf = new SparkConf().setAppName("ETF")//.setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val size = 1000
    val bimg = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)

    val partitions = 8

    val cartAndRadNumbers = Array[Int](5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011, 
     5012, 5013, 5014, 5015, 5016, 5017, 5018, 5019, 5020, 5021, 5022, 5023, 5024, 5025, 5026, 5027, 5028, 5029)//, 
     //6000, 6001, 6002, 6003, 6004, 6005, 6006, 6007, 6008, 6009, 6010, 6011, 6012, 6013, 6014, 6015, 6016, 6017,
     //6018, 6019, 6020, 6021, 6022, 6023, 6024, 6025,6026, 6027, 6028, 6029)

    // val offsets = Array[(Double, Double)]((0,0), (2.0e-5, 0), (-2.0e-5, 0), (2*2.0e-5, 0), (-2*2.0e-5, 0), //5
    //     (0, 2.0e-5), (0, -2.0e-5), (0, 2*2.0e-5))//, (0, -2*2.0e-5))

    val offsets = Array[Int](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29).map(i => (i*2.0e-5,0.0))


    //Creates an RDD with the partition # and the CartAndRad File #
    def divisionOfFiles(partitionNum: Int, cartAndRadNumbersArray: Array[Int]): RDD[(Int, Int)] = {
        val ret = Array.fill(cartAndRadNumbersArray.length)((0,0))
        for (i <- cartAndRadNumbersArray.indices) yield {
            ret(i) = (i % partitionNum, cartAndRadNumbersArray(i))
        }
        sc.parallelize(ret)
    }
    println(divisionOfFiles(partitions, cartAndRadNumbers).collect().toList)


    //Creates RDD[(Int,(Int, Double, Double))] with partition #, CartAndRad File #, offx, and offy
    def giveOffsets(r: RDD[(Int, Int)]) : RDD[(Int,(Int, Double, Double))] = {
        r.map( t => (t._1, (t._2, offsets(t._1)._1, offsets(t._1)._2)))
    }
    println(giveOffsets(divisionOfFiles(8, cartAndRadNumbers)).collect().toList)

    //Uncomment for multiple simulations
  //val particles = (0 until numSims).flatMap { i =>
  //  (CartAndRad.read(new java.io.File(s"/home/mlewis/Rings/AMNS-Moonlets/Moonlet4c/CartAndRad.720$i.bin"))).map(p => GeomSphere(Point(p.x - firstXOffset + i * 2 * cellWidth, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
  //}


    //Map to RDD[(Int, KDTreeGeometry)]
    def createKDTrees(r: RDD[(Int,(Int, Double, Double))]): RDD[(Int, KDTreeGeometry[BoundingSphere])] = {
        r.mapValues(t => GeometrySetup.readRingWithOffset(t._1, t._2, t._3))
    }

    println(createKDTrees(giveOffsets(divisionOfFiles(8, cartAndRadNumbers))).count())

    var kd = createKDTrees(giveOffsets(divisionOfFiles(8, cartAndRadNumbers)))

    println(kd.map(_._2.boundingSphere).collect().toList)

	sc.stop()

  }
}