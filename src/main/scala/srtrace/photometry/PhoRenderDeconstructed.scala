package srtrace.photometry

import java.awt.Graphics
import java.awt.image.BufferedImage
import java.util.Random
import javax.imageio._

import javax.swing._
import java.awt.Color
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import srtrace._
import swiftvis2.raytrace._
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P
import org.apache.spark.storage.StorageLevel


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
    var totalTime = 0.0
    var line = ""
    groupedGeoms.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //2d array of colors
    val colors:Array[Array[RTColor]] = Array.fill(size, size)(RTColor.Black)
    while (line != "q") {
      val start = System.nanoTime() //not including geometry setup time
      println("Rendering...")
      val cRays: RDD[ColorRay] = generatePhotonRays(lights, groupedGeoms)
      //println(s"number of initial rays: ${cRays.count()}")
      val cRayids: RDD[(ColorRay, IntersectData)] = purgeNonCollisions(cRays, groupedGeoms, numPartitions)
      //println(s"number of non-collision rays: ${cRayids.count()}")
      val scatteredCRays: RDD[ColorRay] = scatterPhotonRays(cRayids, view._1)
      //println(s"number of scattered rays: ${scatteredCRays.count()}")
      val cRaysToEye: RDD[ColorRay] = purgeCollisions(scatteredCRays, groupedGeoms, numPartitions)
      //println(s"number of rays to eye: ${cRaysToEye.count()}")
      val pixelColors: Array[(Pixel, RTColor)] = convertRaysToPixelColors(cRaysToEye, view, size).collect()
      //println(s"number of pixelColors: ${pixelColors.length}")


      pixelColors.foreach {
        case (p: Pixel, c: RTColor) => {
          colors(p.x)(p.y) += c
          
        }
      }
      writeToImage(colors, img, bImg)

      frame.repaint()
      frame.paint(frame.getGraphics())
      frame.update(frame.getGraphics())
      frame.setVisible(true)
      val time = (System.nanoTime()-start)*1e-9
      println(time + " seconds this run")
      totalTime = totalTime.toDouble + time
      println(totalTime + " seconds total")
      //frame.update(graphics)
      println("q to quit, or anything else to continue...")
      
      line = readLine()
    }

  }
  def writeToImage(pixels: Array[Array[RTColor]], image: RTImage, bImg: BufferedImage): Unit = {
    val maxPix = pixels.foldLeft(0.0)((m,row) => m max row.foldLeft(0.0)((m2, p) => m2 max p.r max p.g max p.b))
    for (px <- 0 until image.width; py <- 0 until image.height) {
      image.setColor(px, py, (pixels(px)(py) / maxPix * 4.0).copy(a = 1.0)) //switched from 2 to 4 4/1 3:36
    }
    ImageIO.write(bImg, "PNG", new java.io.File(s"photoRender.${System.currentTimeMillis()}.png"))
  }
  /*
    generates rays from each light to random points in the boundingbox at z=0.
  */
  private def generatePhotonRays(
      lights: List[PointLight],
      groupedGeoms: RDD[(Int, KDTreeGeometry[BoundingBox])]
  ): RDD[ColorRay] = {
    groupedGeoms.flatMap(x => {
      val rand = new Random(System.currentTimeMillis())
      val (part, kdTree) = x
      println(s"kdTree bounding box min: ${kdTree.boundingBox.min} kdTree bounding max: ${kdTree.boundingBox.max}")
      //replace with photonSource approach
      lights.flatMap(light => {
        for (i <- 0 until 400000) yield {
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
      cRays.flatMap(ray => (0 to numPartitions).map(x => (x, ray))).repartition(numPartitions)
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
        val scatterPercent = hitGeom.asInstanceOf[ScatterableSphere].fractionScattered(cRay.ray.dir, rayToEye.dir, id)
        ColorRay(cRay.color * scatterPercent, rayToEye)
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
    val second = purgeCollisions2(first)
    val third = purgeCollisions3(second)
    val fourth = purgeCollisions4(third)
    fourth
  }
  def purgeCollisions1(
      cRays: RDD[ColorRay],
      groupedGeom: RDD[(Int, KDTreeGeometry[BoundingBox])],
      numPartitions: Int
  ): RDD[(ColorRay, Option[IntersectData])] = {
    val groupedRays =
      cRays.flatMap(ray => (0 to numPartitions).map(x => (x, ray))).repartition(numPartitions)
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
        val px = ((inRay.dot(right) / fracForward / angle + 1.0) * size / 2).toInt //projecting the direction of the inward ray onto up and right locations 
        val py = ((-inRay.dot(up) / fracForward / angle + 1.0) * size / 2).toInt  //we project inRays onto desired up and rights to calculate 
        //println(s"px: $px, py: $py, fracForward: $fracForward")
        if (px >= 0 && px < size && py >= 0 && py < size) Seq((Pixel(px, py), cRay.color))
        else Seq()
      } else {
        Seq()
      }
    })
  }
}
