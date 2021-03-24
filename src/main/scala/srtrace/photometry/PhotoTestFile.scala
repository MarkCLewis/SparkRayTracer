package srtrace.photometry

import java.awt.image.BufferedImage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import srtrace._
import swiftvis2.raytrace._

object PhotoTestFile extends App {
  def divisionOfFiles(
      sc: SparkContext,
      partitionNum: Int,
      cartAndRadNumbersArray: Seq[Int]
  ): RDD[(Int, Int)] = {
    val ret = Array.fill(cartAndRadNumbersArray.length)((0, 0))
    for (i <- cartAndRadNumbersArray.indices) yield {
      ret(i) = (i, cartAndRadNumbersArray(i))
    }
    sc.parallelize(ret).repartition(partitionNum)
  }

  def giveOffsets(
      sc: SparkContext,
      r: RDD[(Int, Int)],
      offsetArray: IndexedSeq[(Double, Double)]
  ): RDD[(Int, (Int, Double, Double))] = {
    r.map(t => (t._1, (t._2, offsetArray(t._1)._1, offsetArray(t._1)._2)))
  }

  def createKDTrees(
      sc: SparkContext,
      r: RDD[(Int, (Int, Double, Double))]
  ): RDD[(Int, KDTreeGeometry[BoundingBox])] = {
    r.mapValues(t => GeometrySetup.readRingWithOffsetBox(t._1, t._2, t._3))
  }

  val realCartAndRadNumbers = Vector[Int](5000, 5001, 5002, 5003, 5004, 5005,
    5006, 5007, 5008, 5009, 5010, 5011, 5012, 5013, 5014, 5015, 5016, 5017,
    5018, 5019, 5020, 5021, 5022, 5023, 5024, 5025, 5026, 5027, 5028, 5029,
    6000, 6001, 6002, 6003, 6004, 6005, 6006, 6007, 6008, 6009, 6010, 6011,
    6012, 6013, 6014, 6015, 6016, 6017, 6018, 6019, 6020, 6021, 6022, 6023,
    6024, 6025, 6026, 6027, 6028, 6029)

  val kryoConf = new SparkConf().setAppName("photo").setMaster("local[*]")
  
  // kryoConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  // //kryoConf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
  // kryoConf.set("spark.executor.cores", "5")
  // //kryoConf.set("spark.dynamicAllocation.enabled", "true")
  // //kryoConf.set("spark.worker.memory", "16G")
  // //kryoConf.set("spark.executor.cores", "31")
  // kryoConf.set("spark.executor.memory", "15G")
  // //kryoConf.set("spark.dynamicAllocation.initialExecutors", "8")
  // //kryoConf.set("spark.dynamicAllocation.maxExecutors", "8")
  // kryoConf.set("spark.kryoserializer.buffer", "2047M")
  // //kryoConf.set("spark.reducer.maxReqsInFlight", "1")
  // kryoConf.set("spark.shuffle.io.retryWait", "60s")
  // kryoConf.set("spark.shuffle.io.maxRetries", "10")
  
  val sc = new SparkContext(kryoConf)
  
  // kryoConf.registerKryoClasses(
  //   Array(
  //     classOf[Pixel],
  //     classOf[KDTreeGeometry[BoundingBox]],
  //     classOf[GeomSphere],
  //     classOf[ScatterableSphere],
  //     classOf[PointLight],
  //     classOf[Ray],
  //     classOf[IntersectData]
  //   )
  // )
  sc.setLogLevel("WARN")
  sc.statusTracker.getExecutorInfos
  val numPartitions = 1 //args(0).toInt

  val cartAndRadNumbers =
    (0 until (numPartitions.toDouble / realCartAndRadNumbers.length).ceil.toInt)
      .flatMap(_ => realCartAndRadNumbers)
  val size = 800
  val minX = -150
  val maxX = 150
  val usedCartAndRadNumbers = cartAndRadNumbers.take(numPartitions)
  val bImg: BufferedImage =
    new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
  //val lights: List[PointLight] = List(new PointLight(RTColor.White, Point(-2.0, 0.0, 2.0)))
  val lights = List(
    PointLight(new RTColor(0.9, 0.9, 0.9, 1), Point(1e-1, 1, 1e-2)),
    PointLight(new RTColor(0.5, 0.4, 0.1, 1), Point(-1e-1, 1, 1e-2)),
    PointLight(new RTColor(0.8, 0.9, 0.9, 1), Point(1e-1, 1, 1e-2)),
    PointLight(new RTColor(0.2, 0.4, 0.1, 1), Point(-1e-1, 1, 1e-2))
  )
  val n = math.sqrt(numPartitions.toDouble / 10.0).ceil.toInt
  val view = GeometrySetup.topView(10 * n) //.topView()//.standardView()
  //val view = GeometrySetup.standardView()
  val offsets = for (x <- 0 until 10 * n; y <- 0 until n) yield {
    (x * 2.0e-5 - (10 * n - 1) * 1e-5, y * 2e-4 - (n - 1) * 1e-4)
  }
  /*
        algebra zone:
        n * 10n = numPartitions
        10n^2 = numPartitions
        n = sqrt(numPartitions / 10).ceil

   */

  //val offsets = Array[(Double, Double)]((0,0), (2.0e-5, 0), (-2.0e-5, 0), (2*2.0e-5, 0), (-2*2.0e-5, 0), (3*2.0e-5, 0), (-3*2.0e-5, 0), (4*2.0e-5, 0), (-4*2.0e-5, 0), (5*2.0e-5, 0))
  // (0, 2.0e-5), (0, -2.0e-5), (0, 2*2.0e-5), (0, -2*2.0e-5)),

  val geom = createKDTrees(
    sc,
    giveOffsets(
      sc,
      divisionOfFiles(sc, numPartitions, usedCartAndRadNumbers),
      offsets
    )
  )
  //val geom = sc.parallelize(GeometrySetup.smallPhoGeom())
  
  

  PhoRenderDeconstructed.render(sc, geom, lights, bImg, view, size, 1, numPartitions)

  sc.stop()
}
