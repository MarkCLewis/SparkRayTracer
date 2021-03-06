package srtrace

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._
import java.awt.image.BufferedImage
import javax.swing._
import java.awt.Graphics
import org.apache.spark.api.java.StorageLevels

object EricasTestingFile {
  //Creates an RDD with the partition # and the CartAndRad File #
  def divisionOfFiles(sc: SparkContext, partitionNum: Int, cartAndRadNumbersArray: Seq[Int]): RDD[(Int, Int)] = {
      val ret = Array.fill(cartAndRadNumbersArray.length)((0,0))
      for (i <- cartAndRadNumbersArray.indices) yield {
          ret(i) = (i % partitionNum, cartAndRadNumbersArray(i))
      }
      sc.parallelize(ret).repartition(partitionNum)
  }

  def giveOffsets(sc: SparkContext, r: RDD[(Int, Int)], offsetArray: IndexedSeq[(Double, Double)]) : RDD[(Int,(Int, Double, Double))] = {
    
      r.map( t => (t._1, (t._2, offsetArray(t._1)._1, offsetArray(t._1)._2)))
  }

  def createKDTrees(sc: SparkContext, r: RDD[(Int,(Int, Double, Double))]): RDD[(Int, KDTreeGeometry[BoundingSphere])] = {
      r.map(t => (t._1, GeometrySetup.readRingWithOffset(t._2._1, t._2._2, t._2._3)))//.persist(StorageLevels.MEMORY_AND_DISK)
  }
  
  val realCartAndRadNumbers = Vector[Int](5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011, 
     5012, 5013, 5014, 5015, 5016, 5017, 5018, 5019, 5020, 5021, 5022, 5023, 5024, 5025, 5026, 5027, 5028, 5029, 
     6000, 6001, 6002, 6003, 6004, 6005, 6006, 6007, 6008, 6009, 6010, 6011, 6012, 6013, 6014, 6015, 6016, 6017,
     6018, 6019, 6020, 6021, 6022, 6023, 6024, 6025, 6026, 6027, 6028, 6029)
     def main(args: Array[String]) = {
         if (args.length < 2) {
             println("You need to specify a renderer # and how many simulations/partitions.")
             sys.exit(0)
        }
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val kryoConf = new SparkConf().setAppName("ETF")//.setMaster("local[*]")

    // Original Spark settings (commented out 3/30/21)
    // kryoConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // kryoConf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    // kryoConf.set("spark.executor.cores", "5")
    // kryoConf.set("spark.dynamicAllocation.enabled", "true")
    // kryoConf.set("spark.worker.memory", "16G")
    // kryoConf.set("spark.executor.cores", "31")
    // kryoConf.set("spark.executor.memory", "15G")
    // kryoConf.set("spark.dynamicAllocation.initialExecutors", "8")
    // kryoConf.set("spark.dynamicAllocation.maxExecutors", "8")
    // kryoConf.set("spark.kryoserializer.buffer", "2047M")
    // kryoConf.set("spark.reducer.maxReqsInFlight", "1")
    // kryoConf.set("spark.shuffle.io.retryWait", "60s")
    // kryoConf.set("spark.shuffle.io.maxRetries", "10")

    kryoConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //kryoConf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    kryoConf.set("spark.executor.cores", "5")
    //kryoConf.set("spark.dynamicAllocation.enabled", "true")
    //kryoConf.set("spark.worker.memory", "16G")
    //kryoConf.set("spark.executor.cores", "31")
    //kryoConf.set("spark.executor.memory", "15G")
    //kryoConf.set("spark.dynamicAllocation.initialExecutors", "8")
    //kryoConf.set("spark.dynamicAllocation.maxExecutors", "8")
    kryoConf.set("spark.kryoserializer.buffer", "2047M")
    //kryoConf.set("spark.reducer.maxReqsInFlight", "1")
    kryoConf.set("spark.shuffle.io.retryWait", "60s")
    kryoConf.set("spark.shuffle.io.maxRetries", "10")

    kryoConf.registerKryoClasses(Array(classOf[Pixel], classOf[KDTreeGeometry[BoundingSphere]], classOf[GeomSphere], classOf[PointLight], classOf[Ray], classOf[IntersectData]))
    println(kryoConf.toDebugString)
    val sc = new SparkContext(kryoConf)
    sc.setLogLevel("ALL")
    sc.statusTracker.getExecutorInfos
    val numPartitions = args(1).toInt
            
    val cartAndRadNumbers = (0 until (numPartitions.toDouble / realCartAndRadNumbers.length).ceil.toInt).flatMap(_ => realCartAndRadNumbers)
    val size = 800
    val minX = -150
    val maxX = 150
    val usedCartAndRadNumbers = cartAndRadNumbers.take(numPartitions)
    val bimg: BufferedImage = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
    //val lights: List[PointLight] = List(new PointLight(RTColor.White, Point(-2.0, 0.0, 2.0)))
    val lights = List(
        PointLight(new RTColor(1, 1, 1, 1), Point(1, 0, 0.2))
      //PointLight(new RTColor(0.9, 0.9, 0.9, 1), Point(1e-1, 0, 1e-2))/*,
      //PointLight(new RTColor(0.5, 0.4, 0.1, 1), Point(-1e-1, 0, 1e-2))*/
    )
    val n = math.sqrt(numPartitions.toDouble / 10.0).ceil.toInt
    val view = GeometrySetup.topView(10 * n)//.topView()//.standardView()
    val offsets = for(x <- 0 until 10*n; y <- 0 until n) yield {
        (x * 2.0e-5 - (10*n - 1) * 1e-5, y * 2e-4 - (n - 1) * 1e-4)
    }
   

    //val offsets = Array[(Double, Double)]((0,0), (2.0e-5, 0), (-2.0e-5, 0), (2*2.0e-5, 0), (-2*2.0e-5, 0), (3*2.0e-5, 0), (-3*2.0e-5, 0), (4*2.0e-5, 0), (-4*2.0e-5, 0), (5*2.0e-5, 0))
        // (0, 2.0e-5), (0, -2.0e-5), (0, 2*2.0e-5), (0, -2*2.0e-5)), 

    val preGeomStartTime = System.nanoTime()
    val geom = createKDTrees(sc, giveOffsets(sc, divisionOfFiles(sc, numPartitions, usedCartAndRadNumbers), offsets)).persist(StorageLevels.MEMORY_AND_DISK_SER)//.collect()//sc.parallelize(GeometrySetup.randomGeometryActualArr(new scala.util.Random(System.currentTimeMillis), maxX, minX,20,10,10,-10,2, 5)) //actual geometries
    
    // val keyedGeoms: RDD[(Int, GeomSphere)] = geom.map(iGeom => ((iGeom.center.x - minX) / (maxX - minX) * numPartitions).toInt -> iGeom).repartition(numPartitions)
    // val groupedGeoms: RDD[(Int, Geometry)] = keyedGeoms.groupByKey().map { case (i, spheres) => i -> new KDTreeGeometry(spheres.toSeq) }

    println(s"Num partitions = ${geom.getNumPartitions}")
    val geomNoRDD = if (args(0) == "1" || args(0) == "2") new ListScene(geom.collect.map(_._2):_*) else null

    // println(geom.count)
    // println("Partitioning distribution1: "+ geom.glom().map(a => a.length).collect().mkString)
    // println("Partitioning distribution2: "+ geom.glom().collect().map(a => a.mkString).mkString)
    // println("Partitioning distribution3: "+ geom.glom().collect().map(a => a.mkString).mkString)
    // println("Partitioning distribution4: "+ geom.preferredLocations(geom.partitions(0)))
    
    // println("Partitions structure1: " + sc.getAllPools)

    val start = System.nanoTime()


    // println(divisionOfFiles(sc, numPartitions, cartAndRadNumbers).collect().toList)


    //Creates RDD[(Int,(Int, Double, Double))] with partition #, CartAndRad File #, offx, and offy
    // println(giveOffsets(sc, divisionOfFiles(sc, numPartitions, cartAndRadNumbers), offsets).collect().toList)


    //Map to RDD[(Int, KDTreeGeometry)]

    // println(createKDTrees(sc, giveOffsets(sc, divisionOfFiles(sc, numPartitions, cartAndRadNumbers), offsets)).count())


    // var kd = createKDTrees(giveOffsets(divisionOfFiles(8, cartAndRadNumbers)))

    // println(kd.map(_._2.boundingSphere).collect().toList)


    
        // val size = 600
        // val minX = -10
        // val maxX = 10
        // val numPartitions = 8

        // val geom = sc.parallelize(GeometrySetup.randomGeometryActualArr(new scala.util.Random(System.currentTimeMillis), maxX, minX,20,10,10,-10,2, 5)) //actual geometries
        // val keyedGeoms: RDD[(Int, GeomSphere)] = geom.map(iGeom => ((iGeom.center.x - minX) / (maxX - minX) * numPartitions).toInt -> iGeom).repartition(numPartitions)
        // val groupedGeoms: RDD[(Int, Geometry)] = keyedGeoms.groupByKey().map { case (i, spheres) => i -> new KDTreeGeometry(spheres.toSeq) }
        // val light: List[PointLight] = List(new PointLight(RTColor.White, Point(-2.0, 0.0, 2.0)))
        // val bimg: BufferedImage = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
        // val view = GeometrySetup.standardView()
        //  def render(geom: RDD[GeomSphere], light: List[PointLight], bImg: BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays:Int = 1, numPartitions:Int = 8, minX:Double, maxX:Double): Unit = {

    args(0) match {
        case "1" => 
            Renderer1.render(sc, geomNoRDD, lights, bimg, view, size, 1, numPartitions)
        case "2" =>
            Renderer2.render(sc, geomNoRDD, lights, bimg, view, size, 1, numPartitions)
        case "4" =>
            Renderer3b.render(sc, geom, lights, bimg, view, size, 1, numPartitions)
        case _ =>
            Renderer3.render(sc, geom, lights, bimg, view, size, 1, numPartitions)
    }

        println("Not including geometry: " + (System.nanoTime()-start)*1e-9 + " seconds")
        println("Including geometry: " + (System.nanoTime()-preGeomStartTime)*1e-9 + " seconds")

        val frame = new JFrame {
            override def paint(g: Graphics): Unit = {
                g.drawImage(bimg, 0, 0, null)
            }
        }
        frame.setSize(size, size)
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
        frame.setVisible(true)
        
    

	sc.stop()

  }
}
