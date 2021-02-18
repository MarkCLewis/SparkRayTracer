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

case class ColorRay(color: RTColor, ray: Ray)
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
    }

    frame.setSize(size, size)
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    frame.setVisible(true)
    
    println("Press Enter to continue rendering.")

    if (readLine() == "") {
        groupedGeoms.persist() //defaults to memeoryAndDisk
        val cRays: RDD[ColorRay] = generatePhotonRays(lights, groupedGeoms)
        val cRayids: RDD[(ColorRay, IntersectData)] = purgeNonCollisions(cRays, groupedGeoms, numPartitions)
        val scatteredCRays: RDD[ColorRay] = scatterPhotonRays(cRayids, view._1)
        val cRaysToEye: RDD[ColorRay] = purgeCollisions(scatteredCRays, groupedGeoms)
        val pixelColors: Array[(Pixel, RTColor)] = convertRaysToPixelColors(cRaysToEye, view, size).collect()
        pixelColors.foreach{case (p: Pixel, c:RTColor) => {
          bImg.setRGB(p.x, p.y, c.toARGB)
        }}
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
      for (light <- lights) yield {
        ColorRay(
          light.col,
          Ray(
            light.point,
            (kdTree.boundingBox.min.toVect + Point(
              rand.nextInt(10),
              rand.nextInt(10),
              0
            ).toVect)
          )
        )
      }
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
    val groupedRays = cRays.flatMap(ray => (1 to 8).map(x => (x, ray)))
    groupedGeom
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
        (part, (cRay, oid.get))
      })
      .reduceByKey(
        (v1: (ColorRay, IntersectData), v2: (ColorRay, IntersectData)) => {
          val (cRay1: ColorRay, id1: IntersectData) = v1
          val (cRay2: ColorRay, id2: IntersectData) = v2
          if (id1.time <= id2.time) v1 else v2
        }
      )
      .values
  }

  /*
    "scatters" photon rays using the ScatterableGeometry's scatter function
   */
  private def scatterPhotonRays(
      hitCRays: RDD[(ColorRay, IntersectData)],
      eye: Point
  ): RDD[ColorRay] = {
    hitCRays.map({ case ((cRay:ColorRay, id:IntersectData)) => 
      val hitPoint = id.point
      val hitGeom = id.geom
      ColorRay(cRay.color, Ray(hitPoint, eye))
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
      })
      .reduceByKey(
        (
            v1: (ColorRay, Option[IntersectData]),
            v2: (ColorRay, Option[IntersectData])
        ) => {
          val (cRay1: ColorRay, oid1: Option[IntersectData]) = v1
          val (cRay2: ColorRay, oid2: Option[IntersectData]) = v2
          if (oid1 == None && oid2 == None) v1
          else if (oid1 == None && oid2 != None) v2
          else if (oid1 != None && oid2 == None) v1
          else v1
        }
      )
      .filter(x => {
        val (part: Int, (cRay: ColorRay, oid: Option[IntersectData])) = x
        oid == None
      })
      .map(x => {
        val (part: Int, (cRay: ColorRay, oid: Option[IntersectData])) = x
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
      val viewOID = GeomBox(view._2, view._2 + view._3 + view._4, {case p:Point => RTColor.Black}, {case p:Point => Double.MinValue}).intersect(cRay.ray)
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
  private def calcPixel(viewDiff: Vect, view: (Point, Point, Vect, Vect), size: Int):Pixel = {
    val topLeft = view._2
    val right = view._3
    val down = view._4
    val x = viewDiff.x - topLeft.x
    val y = viewDiff.y - topLeft.y
    Pixel((x / right.x * size).toInt, (y / down.y * size).toInt)
  }
}
