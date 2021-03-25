package srtrace.photometry

import java.awt.Graphics
import java.awt.image.BufferedImage
import java.util.Random

import javax.swing._
import java.awt.Color
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import srtrace._
import swiftvis2.raytrace._

object PhoRenderDeconstructed {
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
      //println("taken 10 crays: " + cRays.take(10).mkString("\n"))
      //println("colorRays: " + cRays.count())
      val cRayids: RDD[(ColorRay, IntersectData)] =
        purgeNonCollisions(cRays, groupedGeoms, numPartitions)
      //println("rays to geometry: " + cRayids.count())
      val scatteredCRays: RDD[ColorRay] = scatterPhotonRays(cRayids, view._1)
      //println("scattered rays: " + scatteredCRays.count())
      val cRaysToEye: RDD[ColorRay] =
        purgeCollisions(scatteredCRays, groupedGeoms, numPartitions)
      //println("rays to eye: " + cRaysToEye.count())
      val pixelColors: Array[(Pixel, RTColor)] =
        convertRaysToPixelColors(cRaysToEye, view, size).collect()
      //println("pixels: " + pixelColors.length)
      pixelColors.foreach {
        case (p: Pixel, c: RTColor) => {
          if (c != RTColor.Black) {
            println(c + ", " + p)
          }
          println("point: " + p + ", color: " + c)
          val col = bImg.getRGB(p.x, p.y)
          
          bImg.setRGB(p.x, p.y, col + c.toARGB)
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
      println(s"kdTree bounding box min: ${kdTree.boundingBox.min} kdTree bounding max: ${kdTree.boundingBox.max}")
      //replace with photonSource approach
      lights.flatMap(light => {
        for (i <- 0 until 4000) yield {
          val randomLocation = Point(
            kdTree.boundingBox.min.x + rand.nextDouble * (kdTree.boundingBox.max.x - kdTree.boundingBox.min.x),
            kdTree.boundingBox.min.y + rand.nextDouble * (kdTree.boundingBox.max.y - kdTree.boundingBox.min.y),
            0
          )
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
    val groupedRays =
      cRays.flatMap(ray => (0 to numPartitions).map(x => (x, ray)))
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
    partitionerTarget
      .reduceByKey(
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
        val rayToEye = Ray(hitPoint + id.norm * 0.0001 * geomSize, eye)
        //val scatterPercent = hitGeom.asInstanceOf[ScatterableSphere].fractionScattered(cRay.ray.dir, rayToEye.dir, id)
        ColorRay(cRay.color, rayToEye)
    })
  }

  /*
    checks rays for collisions with geometry and removes all collisions
   */
  private def purgeCollisions(
      cRays: RDD[ColorRay],
      groupedGeom: RDD[(Int, KDTreeGeometry[BoundingBox])],
      numPartitions: Int
  ): RDD[ColorRay] = {
    val first = purgeCollisions1(cRays, groupedGeom, numPartitions)
    // println("FIRST")
    // println(first.count())
    // println(first.take(10).mkString(", "))
    val second = purgeCollisions2(first)
    // println("SECOND")
    // println(second.count())
    // println(second.take(10).mkString(", "))
    val third = purgeCollisions3(second)
    // println("THIRD")
    // println(third.count())
    // println(third.take(10).mkString(", "))
    val fourth = purgeCollisions4(third)
    // println("FOURTH")
    // println(fourth.count())
    // println(fourth.take(10).mkString(", "))
    fourth
  }
  def purgeCollisions1(
      cRays: RDD[ColorRay],
      groupedGeom: RDD[(Int, KDTreeGeometry[BoundingBox])],
      numPartitions: Int
  ): RDD[(ColorRay, Option[IntersectData])] = {
    val groupedRays =
      cRays.flatMap(ray => (0 to numPartitions).map(x => (x, ray)))
    // println("grouped rays!")
    // println(groupedRays.take(5).mkString(", "))
    // println("grouped geoms!")
    // println(groupedGeom.take(5).mkString(", "))
    groupedGeom
      .join(groupedRays)
      .map(x => {
        val (part: Int, (geom: KDTreeGeometry[BoundingBox], cRay: ColorRay)) = x
        (part, (cRay, geom intersect cRay.ray))
      })
      .values
  }
  def purgeCollisions2(rdd: RDD[(ColorRay, Option[IntersectData])]) = {
    rdd.reduceByKey(
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
  }
  def purgeCollisions3(rdd: RDD[(ColorRay, Option[IntersectData])]) = {
    rdd.filter(x => {
      val (cRay: ColorRay, oid: Option[IntersectData]) = x
      oid == None
    })
  }
  def purgeCollisions4(rdd: RDD[(ColorRay, Option[IntersectData])]) = {
    rdd.map(x => {
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
    val up = (-view._4).normalize
    val right = view._3.normalize
    val forward = up.cross(right)//view._3.cross(view._4).normalize
    val angle = 1.0 //.707
    //println(s"up: $up right: $right forward: $forward")
    cRays.flatMap((cRay: ColorRay) => {
      //println("new ray")
      val inRay = cRay.ray.dir.normalize
      val fracForward = inRay.dot(forward)
      //println(s"cRay: $cRay")
      //println(s"inRay: $inRay")
      //println(fracForward)
      if (fracForward < 0.0) {
        val px = ((inRay.dot(right) / fracForward / angle + 1.0) * size / 2).toInt
        val py = ((-inRay.dot(up) / fracForward / angle + 1.0) * size / 2).toInt //dir is a ray from the intersect point to the eyeball
        //println(s"px: $px, py: $py, fracForward: $fracForward")
        if (px >= 0 && px < size && py >= 0 && py < size) Seq((Pixel(px, py), cRay.color))
        else Seq()
      } else {
        Seq()
      }
    })
  }
//eye, topLeft, right, down
//(Point(0.0, 0.0, distMult*1e-5), Point(-1e-5, 1e-5, (distMult-1)*1e-5), Vect(2 * 1e-5, 0, 0), Vect(0, -2 * 1e-5, 0))
//   private def calcPixel(
//       viewDiff: Vect,
//       view: (Point, Point, Vect, Vect),
//       size: Int
//   ): Pixel = {
//     // val topLeft = view._2
//     // val right = view._3
//     // val down = view._4
//     // val x = viewDiff.x / right.magnitude
//     // val y = (viewDiff.z / down.magnitude).abs //(viewDiff.y - topLeft.y).abs//((topLeft.y - viewDiff.y.abs).abs)
//     // //Pixel(((x / right.magnitude) * (size -1)).toInt, ((y.abs / down.magnitude.abs) * (size - 1)).toInt)
//     // Pixel((x * size).toInt, (y * size).toInt)

//   }
}
