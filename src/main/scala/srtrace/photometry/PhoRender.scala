package srtrace.photometry

import java.awt.image.BufferedImage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._
import java.net.InetAddress
import java.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import java.awt.image.BufferedImage
import javax.swing._
import java.awt.Graphics
import srtrace._
import org.apache.spark.RangePartitioner

case class ColorRay(color: RTColor, ray: Ray) extends Comparable[ColorRay] {
  def compareTo(that: ColorRay): Int = {
    val maxed = this.ray.p1.max(that.ray.p1)
    if(maxed == this.ray.p1) 1
    else if (this == that) 0
    else -1
  }
}
object PhoRender {
  def render(
      sc: SparkContext,
      groupedGeoms: RDD[(Int, KDTreeGeometry[BoundingBox])],
      lights: List[PointLight],
      bImg: BufferedImage,
      view: (Point, Point, Vect, Vect),
      size: Int,
      numRays: Int = 1,
      numPartitions: Int
  ): Unit = {

    val img = new RTImage {
      def width = bImg.getWidth()

      def height = bImg.getHeight()

      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bImg.setRGB(x, y, color.toARGB)
      }
    }

    for (i <- 0 until size; j <- 0 until size) bImg.setRGB(i, j, 0xFF000000)

    val frame = new JFrame {
      override def paint(g: Graphics): Unit = {
        g.drawImage(bImg, 0, 0, null)
      }
      override def update(g: Graphics): Unit = {
        g.drawImage(bImg, 0, 0, null)
      }
    }
    //val graphics = frame.getGraphics()
    frame.setSize(size, size)
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    frame.setVisible(true)

    var line = ""

    while (line != "q") {
      println("Rendering...")
      groupedGeoms.persist() //defaults to memeoryAndDisk
      val cRays: RDD[ColorRay] = generatePhotonRays(lights, groupedGeoms)
      println("colorRays: " + cRays.count())
      val cRayids: RDD[(ColorRay, IntersectData)] =
        purgeNonCollisions(cRays, groupedGeoms, numPartitions)
      println("rays to geometry: " + cRayids.count())
      val scatteredCRays: RDD[ColorRay] = scatterPhotonRays(cRayids, view._1)
      println("scattered rays: " + scatteredCRays.count())
      val cRaysToEye: RDD[ColorRay] =
        purgeCollisions(scatteredCRays, groupedGeoms)
      println("rays to eye: " + cRaysToEye.count())
      val pixelColors: Array[(Pixel, RTColor)] =
        convertRaysToPixelColors(cRaysToEye, view, size).collect()
      println("pixels: " + pixelColors.length)
      pixelColors.foreach {
        case (p: Pixel, c: RTColor) => {
          if(c != RTColor.Black) { println(c + ", " + p) }
          println("point: " + p + ", color: " + c)
          bImg.setRGB(p.x, p.y, c.toARGB)
        }
      }
      frame.repaint()
      frame.paint(frame.getGraphics())
      frame.update(frame.getGraphics())
      frame.setVisible(true)

      //frame.update(graphics)
      println("q to quit, or anything else to continue...")
      line = readLine()
    }

  }

  /*
    generates rays from each light to random points in the boundingbox at z=0.
  */
  private def generatePhotonRays(
      lights: List[PointLight],
      groupedGeoms: RDD[(Int, KDTreeGeometry[BoundingBox])]
  ): RDD[ColorRay] = {
    val rand = new Random(System.currentTimeMillis())
    groupedGeoms.values.flatMap(kdTree => {
      //replace with photonSource approach
      lights.flatMap(light => {
        for (i <- 0 until 10000) yield {
          val boxDiff = kdTree.boundingBox.max.max(kdTree.boundingBox.min) - kdTree.boundingBox.min.min(kdTree.boundingBox.max)//(kdTree.boundingBox.max - kdTree.boundingBox.min)
          val minPoint = kdTree.boundingBox.min.min(kdTree.boundingBox.max)
          val randomLocation = minPoint + Vect(rand.nextDouble * boxDiff.x, rand.nextDouble * boxDiff.y, rand.nextDouble * boxDiff.z)
          ColorRay(
            light.col,
            Ray(
              light.point,
              randomLocation
            )
          )
        }
      })
    })
  }

  /*
    check the rays for collisions against geometry and remove all non-collisions
   */
  private def purgeNonCollisions(
      cRays: RDD[ColorRay],
      groupedGeom: RDD[(Int, KDTreeGeometry[BoundingBox])],
      numPartitions: Int
  ): RDD[(ColorRay, IntersectData)] = {
    val groupedRays = cRays.flatMap(ray => (0 to numPartitions).map(x => (x, ray)))
    val partitionerTarget = groupedGeom
      .join(groupedRays)
      .map(x => {
        val (part: Int, (geom: KDTreeGeometry[BoundingBox], cRay: ColorRay)) = x
        (part, (cRay, geom intersect cRay.ray))
      })
      .filter(x => {
        val (part, (cRay: ColorRay, oid: Option[IntersectData])) = x
        oid != None
      })
      .map(x => {
        val (part, (cRay: ColorRay, oid: Option[IntersectData])) = x
        (cRay, oid.get)
      })
      val rangeParter = new RangePartitioner(numPartitions, partitionerTarget, false)
      partitionerTarget.partitionBy(rangeParter).reduceByKey(
        (id1: IntersectData, id2: IntersectData) => {
          if (id1.time <= id2.time) id1 else id2
        }
      )
  }

  /*
    "scatters" photon rays using the ScatterableGeometry's scatter function
   */
  private def scatterPhotonRays(
      hitCRays: RDD[(ColorRay, IntersectData)],
      eye: Point
  ): RDD[ColorRay] = {
    hitCRays.map({
      case ((cRay: ColorRay, id: IntersectData)) =>
        val geomSize = id.geom.boundingSphere.radius
        val hitPoint = id.point
        val hitGeom = id.geom
        val rayToEye = Ray(hitPoint + id.norm * 0.0001, eye)
        //val scatterPercent = hitGeom.asInstanceOf[ScatterableSphere].fractionScattered(cRay.ray.dir, rayToEye.dir, id)
        ColorRay(cRay.color, rayToEye)
    })
  }

  /*
    checks rays for collisions with geometry and removes all collisions
   */
  private def purgeCollisions(
      cRays: RDD[ColorRay],
      groupedGeom: RDD[(Int, KDTreeGeometry[BoundingBox])]
  ): RDD[ColorRay] = {
    val groupedRays = cRays.flatMap(ray => (1 to 8).map(x => (x, ray)))
    groupedGeom
      .join(groupedRays)
      .map(x => {
        val (part: Int, (geom: KDTreeGeometry[BoundingBox], cRay: ColorRay)) = x
        (part, (cRay, geom intersect cRay.ray))
      }).values
      .reduceByKey(
        (
            oid1: Option[IntersectData],
            oid2: Option[IntersectData]
        ) => {
          if (oid1 == None && oid2 == None) oid1
          else if (oid1 == None && oid2 != None) oid2
          else if (oid1 != None && oid2 == None) oid1
          else oid1
        }
      )
      .filter(x => {
        val (cRay: ColorRay, oid: Option[IntersectData]) = x
        oid == None
      })
      .map(x => {
        val (cRay: ColorRay, oid: Option[IntersectData]) = x
        cRay
      })

  }

  /*
    determines the pixel location of this ray's impact, and then stores that location with the ray's color.
   */
  private def convertRaysToPixelColors(
      cRays: RDD[ColorRay],
      view: (Point, Point, Vect, Vect), //eye, topLeft, right, down
      size: Int
  ): RDD[(Pixel, RTColor)] = {
    cRays.flatMap((cRay: ColorRay) => {
      val viewOID = GeomBox(view._2, view._2 + view._3 + view._4, {
        case p: Point    => RTColor.Black
      }, { case p: Point => Double.MinValue }).intersect(cRay.ray)
      viewOID match {
        case Some(viewId: IntersectData) => {
          val viewIPoint = viewId.point
          val viewDiff = viewIPoint - view._2
          Seq((calcPixel(viewDiff, view, size), cRay.color))
        }
        case None => {
          Seq()
        }
      }

    })
  }
//eye, topLeft, right, down
//(Point(0.0, 0.0, distMult*1e-5), Point(-1e-5, 1e-5, (distMult-1)*1e-5), Vect(2 * 1e-5, 0, 0), Vect(0, -2 * 1e-5, 0))
  private def calcPixel(
      viewDiff: Vect,
      view: (Point, Point, Vect, Vect),
      size: Int
  ): Pixel = {
    val topLeft = view._2
    val right = view._3
    val down = view._4
    val x = viewDiff.x / right.magnitude
    val y = (viewDiff.z / down.magnitude).abs//(viewDiff.y - topLeft.y).abs//((topLeft.y - viewDiff.y.abs).abs)
    Pixel(((x / right.magnitude) * (size -1)).toInt, ((y.abs / down.magnitude.abs) * (size - 1)).toInt)
    Pixel((x * size).toInt, (y * size).toInt)
  }
}
