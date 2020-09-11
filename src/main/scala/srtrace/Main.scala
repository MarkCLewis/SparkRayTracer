package srtrace

import java.awt.Graphics
import java.awt.image.BufferedImage

import javax.swing.{JFrame, WindowConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import swiftvis2.raytrace._

object Main {
  def main(args: Array[String]):Unit = {
    val size = 600
    val minX = -10
    val maxX = 10
    val numPartitions = 8
    val conf = new SparkConf().setAppName("Renderer3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val geom = sc.parallelize(GeometrySetup.randomGeometryActualArr(new scala.util.Random(System.currentTimeMillis), maxX, minX,20,10,10,-10,2, 5)) //actual geometries
    val keyedGeoms: RDD[(Int, GeomSphere)] = geom.map(iGeom => ((iGeom.center.x - minX) / (maxX - minX) * numPartitions).toInt -> iGeom).repartition(numPartitions)
    val groupedGeoms: RDD[(Int, KDTreeGeometry[BoundingSphere])] = keyedGeoms.groupByKey().map { case (i, spheres) => i -> new KDTreeGeometry(spheres.toSeq) }
//    val light: List[PointLight] = List(new PointLight(RTColor.Blue, Point(-2.0, 0.0, 2.0)), new PointLight(RTColor(1.0,0.0,0.0,0.1), Point(12.0, 0.0, 2.0)))
    val light: List[PointLight] = List(PointLight(new RTColor(0.9, 0.9, 0.9, 1), Point(1e-1, 0, 1e-2)), PointLight(new RTColor(0.5, 0.4, 0.1, 1), Point(-1e-1, 0, 1e-2)))
    val bimg: BufferedImage = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
    val view = GeometrySetup.standardView()
    //  def render(geom: RDD[GeomSphere], light: List[PointLight], bImg: BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays:Int = 1, numPartitions:Int = 8, minX:Double, maxX:Double): Unit = {

    Renderer3.render(sc, groupedGeoms, light, bimg, view, size, 1, 8)

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
