package srtrace

import java.awt.image.BufferedImage

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
  }
}
