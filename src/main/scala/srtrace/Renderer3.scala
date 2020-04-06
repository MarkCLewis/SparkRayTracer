package srtrace

import java.awt.Graphics
import java.awt.image.BufferedImage
import javax.swing._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import swiftvis2.raytrace._

object Renderer3 {
  def main(args:Array[String]) = {
    val conf = new SparkConf().setAppName("Renderer1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val size = 1000
    val geom = GeometrySetup.randomGeometryArr(new scala.util.Random(System.currentTimeMillis), 10,-10,20,10,10,-10,2,10) //new GeomSphere(Point(1.0, 5.0, 0.0), 1.0, p => RTColor(0xFFFFFF00), p => 0.0)
    val broadcastGeom = sc.broadcast(geom)
    val light:List[PointLight] = List(new PointLight(RTColor.Blue, Point(-2.0, 0.0, 2.0)))
    val bimg = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
    for(i <- 0 until size; j <- 0 until size) bimg.setRGB(i, j, 0xFF000000)
    val img = new RTImage {
      def width = bimg.getWidth()
      def height = bimg.getHeight()
      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bimg.setRGB(x, y, color.toARGB)
      }
    }
    val numRays = 2



    //GeometryRDD
    val minX = -10.0
    val maxX = 10.0 //for use in arrGeoms
    val diff = maxX - minX
    val numPartitions = 8
    val interval = diff / numPartitions
    val arrGeoms:RDD[GeomSphere] = sc.parallelize(GeometrySetup.randomGeometryActualArr(new scala.util.Random(System.currentTimeMillis), maxX, minX,20,10,10,-10,2,1000))
    val keyedGeoms:RDD[(Int, GeomSphere)] = arrGeoms.map(iGeom => ((iGeom.center.x - minX) / diff * numPartitions).toInt -> iGeom).repartition(numPartitions)
    val groupedGeoms:RDD[(Int, Geometry)] = keyedGeoms.groupByKey().map{ case (i, spheres) => i -> new KDTreeGeometry(spheres.toSeq)}
  }
  

  




  //designed to implement the (Int, RayData) step of the flowchart.
  //Sidenote: RayData isn't actually a type, I suppose it's a kind of
  //shorthand we're using to describe ((Int, Int), Ray)?
  def createPartitionedRayData():RDD[(Int, ((Int, Int), Ray))] = {
    ???
  }



  def makeRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numRays: Int): 
    RDD[((Int, Int), Ray)] = {
    //make rays in a scala collection
    val aspect = img.width.toDouble/img.height
    val rays = for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
      ((i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width) 
        + down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
    }
    sc.parallelize(rays)
  }
  def intersectEye(rays: RDD[((Int, Int), Ray)], bGeom: Broadcast[Geometry]):RDD[((Int, Int), (Ray, Option[IntersectData]))] = {
    rays.mapValues(ray => (ray, (bGeom.value) intersect ray))
  }
  def explodeLights(rayids: RDD[((Int, Int), (Ray, Option[IntersectData]))], lights:List[PointLight]): RDD[((Int, Int), (IntersectData, PointLight))] = {
    rayids.flatMap(rayid => {
      explodeLight(rayid, lights)
    })
  }
  def explodeLight(rayid:((Int, Int), (Ray, Option[IntersectData])), lights:List[PointLight]): List[((Int, Int), (IntersectData, PointLight))] = {
    val ((x:Int, y:Int), (ray:Ray, oid:Option[IntersectData])) = rayid
    oid match {
      case None => List[((Int, Int), (IntersectData, PointLight))]()
      case Some(id:IntersectData) => {
        val ret:List[((Int, Int), (IntersectData, PointLight))] = lights.map(light => {
          ((x, y), ((id, light)))
        })
        ret
      }
    }
  }
  def calcLightColors(idColors: RDD[((Int, Int), (IntersectData, PointLight))], bGeom:Broadcast[Geometry]): RDD[((Int, Int), RTColor)] = {
    val groupedByLocation:RDD[((Int, Int), Iterable[(IntersectData, PointLight)])] = idColors.groupByKey()
    val mapped:RDD[((Int, Int), RTColor)] = groupedByLocation.mapValues(findLightRayValues(_, bGeom))
    mapped
  }
  def findLightRayValues(lightids: Iterable[(IntersectData, PointLight)], bGeom:Broadcast[Geometry]): RTColor = {
    var startColor = RTColor(0, 0, 0, 1)
    for((id:IntersectData, light:PointLight) <- lightids) {
      val outRay = Ray(id.point + id.norm * 0.0001 * id.geom.boundingSphere.radius, light.point)
      val oid = bGeom.value.intersect(outRay)
      oid match {
        case None => startColor = startColor + light.color(id, bGeom.value)
        case Some(lid) => startColor = startColor + RTColor(0, 0, 0, 1)
      }
    }
    startColor
  }



  def combineAndSetColors(colors: RDD[((Int, Int), RTColor)], img: RTImage, numRays: Int): Unit = {
    val combinedColors = colors.reduceByKey( _ + _ ).mapValues(rt => (rt/numRays).copy(a=1.0)).collect
    for( ((x,y) ,c) <- combinedColors.par) {
      img.setColor(x,y,c)
    }
  }


  def castRay(ray: Ray, bGeom: Broadcast[Geometry], lights: List[Light], cnt: Int): RTColor = {
    if (cnt > 5) new RTColor(0, 0, 0, 1)
    else {
      val oid = (bGeom.value) intersect ray
      oid match {
        case None => RTColor.Black
        case Some(id) => {
          val geomSize = id.geom.boundingSphere.radius
          val lightColors = for (light <- lights) yield light.color(id, bGeom.value)
          val refColor = if (id.reflect > 0) {
            val refRay = new Ray(id.point + id.norm * 0.0001 * geomSize, ray.dir - id.norm * 2 * (id.norm dot ray.dir))
            castRay(refRay, bGeom, lights, cnt + 1)
          } else new RTColor(0, 0, 0, 1)
          id.color * (lightColors.foldLeft(new RTColor(0, 0, 0, 1))(_ + _)) + refColor
        }
      }
    }
  }
}

//just went ahead and made this, just in case it turns out to be useful in the future.
/*
import org.apache.spark.Partitioner
class CustomGeomPartitioner(nSlices:Int) extends Partitioner {
  override def numPartitions:Int = this.nSlices
  override def getPartition(key:Any):Int = {
    val (k:Int, rayData) = key
    k
  }
  override def equals(other:Any):Boolean = other match {
    case part:CustomerGeomPartitioner => part.numPartitions == numPartitions
    case _ => false
  }
}
*/