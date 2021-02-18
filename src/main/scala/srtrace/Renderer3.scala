package srtrace

import java.awt.image.BufferedImage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._
import java.net.InetAddress
import org.apache.spark.api.java.StorageLevels

// TODO: Put in some meaningful case classes so we don't have tuples everywhere.
case class Pixel(x:Int, y:Int)

object Renderer3 {

  def render(sc: SparkContext, groupedGeoms: RDD[(Int, KDTreeGeometry[BoundingSphere])], light: List[PointLight], bImg: BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays: Int = 1, numPartitions: Int): Unit = {

    println("Partitioning distribution5: "+ sc.statusTracker.getExecutorInfos.map(a => "||"+ a.host() +", "+ a.numRunningTasks()+"||").mkString)
    val img = new RTImage {
      def width = bImg.getWidth()

      def height = bImg.getHeight()

      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bImg.setRGB(x, y, color.toARGB)
      }
    }
    for (i <- 0 until size; j <- 0 until size) bImg.setRGB(i, j, 0xFF000000)

    println("Partitioning distribution6: "+ sc.statusTracker.getExecutorInfos.map(a => "||"+ a.host() +", "+ a.numRunningTasks()+"||").mkString)
    val dupedRays: RDD[(Int, (Pixel, Ray))] = makeNPartitionsRays(sc, view._1, view._2, view._3, view._4, img, numPartitions, numRays).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"duped ${dupedRays.getNumPartitions}")
    val rayGeoms: RDD[(Int, (KDTreeGeometry[BoundingSphere], (Pixel, Ray)))] = groupedGeoms.join(dupedRays).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"rayGeoms ${rayGeoms.getNumPartitions}")
    val rayoids: RDD[(Int, (Pixel, (Ray, Option[IntersectData])))] = intersectEye(rayGeoms).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"rayoids ${rayoids.getNumPartitions}")
    val realIntersects: RDD[(Pixel, (Ray, Option[IntersectData]))] = departitionAndFindShortest(rayoids).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"realIntersects ${realIntersects.getNumPartitions}")
    val idLights: RDD[((Int, Pixel), (IntersectData, PointLight))] = explodeLights(realIntersects, light).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"idLights ${idLights.getNumPartitions}")
    val lightColors: RDD[(Int, ((Int, Pixel), Ray, RTColor, IntersectData))] = makeRaysToLights(idLights, numPartitions).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"lightColors ${lightColors.getNumPartitions}")
    val idRDD: RDD[(Pixel, (Ray, Option[IntersectData], RTColor, IntersectData))] = checkLightRaysForGeomIntersections(lightColors, groupedGeoms).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"idRDD ${idRDD.getNumPartitions}")
    val colors: RDD[(Pixel, RTColor)] = generateColors(idRDD).persist((StorageLevels.MEMORY_AND_DISK))
    println(s"colors ${colors.getNumPartitions}")

    println("Partitioning distribution7: "+ sc.statusTracker.getExecutorInfos.map(a => "||"+ a.host() +", "+ a.numRunningTasks()+"||").mkString)
    combineAndSetColors(colors, img, numRays)
  }

  private def makeNPartitionsRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numPartitions: Int, numRays: Int):
  RDD[(Int, (Pixel, Ray))] = {
    //make rays in a scala collection
    val aspect = img.width.toDouble / img.height
    val rays = (for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
      (Pixel(i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width)
        + down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
    }).flatMap(x => {
      for (i <- 0 until numPartitions) yield {
        i -> x
      }
    })
    sc.parallelize(rays).repartition(numPartitions)
  }
  
  private def intersectEye(rayGeoms: RDD[(Int, (KDTreeGeometry[BoundingSphere], (Pixel, Ray)))]): RDD[(Int, (Pixel, (Ray, Option[IntersectData])))] = {
    rayGeoms.map(indiv => {
      val (n, (geom, (pix, ray))) = indiv
      println("eye", n, geom.boundingBox.min, java.net.InetAddress.getLocalHost().getHostAddress())
      (n, (pix, (ray, (geom) intersect ray)))
    })
  }

  // Collapse to one intersect per pixel. (Minimum by id.time.)
  // Explode needs to re-distribute. Explode in lights and then in geometry partitions.
  private def departitionAndFindShortest(intersects: RDD[(Int, (Pixel, (Ray, Option[IntersectData])))]): RDD[(Pixel, (Ray, Option[IntersectData]))] = {
    val noPartitions = intersects.values
    val zeroRay = Ray(Point(0, 0, 0), Vect(0, 0, 0))
    noPartitions.aggregateByKey((zeroRay, None: Option[IntersectData]))({ case ((ray, minID), (cRay, cID)) =>
      if (minID.isEmpty || cID.nonEmpty && cID.get.time > minID.get.time) (cRay, cID) else (ray, minID)
    }, { 
      case ((r1, oid1), (r2, None)) => (r1, oid1)
      case ((r1, None), (r2, oid2)) => (r2, oid2)
      case ((r1, Some(id1)), (r2, Some(id2))) => if (id1.time < id2.time) (r1, Some(id1)) else (r2, Some(id2))
    })
  }

  private def explodeLights(rayids: RDD[(Pixel, (Ray, Option[IntersectData]))], lights: List[PointLight]): RDD[((Int, Pixel), (IntersectData, PointLight))] = {
    rayids.flatMap(rayidi => {
      val rayid = rayidi
      explodeLight(rayid, lights)
    })
  }

  private def explodeLight(rayid: (Pixel, (Ray, Option[IntersectData])), lights: List[PointLight]): Seq[((Int, Pixel), (IntersectData, PointLight))] = {
    val (pix, (ray: Ray, oid: Option[IntersectData])) = rayid
    oid match {
      case None => List[((Int, Pixel), (IntersectData, PointLight))]()
      case Some(id: IntersectData) => {
        val ret: List[((Int, Pixel), (IntersectData, PointLight))] = lights.zipWithIndex.map { case (light, index) =>
          ((index, pix), ((id, light)))
        }
        ret
      }
    }
  }

  private def makeRaysToLights(idLights: RDD[((Int, Pixel), (IntersectData, PointLight))], numPartitions: Int): RDD[(Int, ((Int, Pixel), Ray, RTColor, IntersectData))] = {
    val repart: RDD[(Int, ((Int, Pixel), (IntersectData, PointLight)))] = idLights.flatMap(x => {
      for (i <- 0 until numPartitions) yield {
        (i, x)
      }
    }).repartition(numPartitions)
    val lightRays: RDD[(Int, ((Int, Pixel), Ray, RTColor, IntersectData))] = repart.map(elem => {
      val (n, pixIDLights) = elem
      val ((index, pix), (id, pl)) = pixIDLights
      val loc = id.point
      val li = pl.point
      val c = pl.col
      (n, ((index, pix), Ray(loc + id.norm * 0.0001 * id.geom.boundingSphere.radius, li), c, id))
    })
    lightRays
  }

  private def checkLightRaysForGeomIntersections(lightRays: RDD[(Int, ((Int, Pixel), Ray, RTColor, IntersectData))], geom: RDD[(Int, KDTreeGeometry[BoundingSphere])]): RDD[(Pixel, (Ray, Option[IntersectData], RTColor, IntersectData))] = {
    val joined: RDD[(Int, (KDTreeGeometry[BoundingSphere], ((Int, Pixel), Ray, RTColor, IntersectData)))] = geom.join(lightRays)
    val withOIDs: RDD[(Int, ((Int, Pixel), (Ray, Option[IntersectData], RTColor, IntersectData)))] = joined.map(elem => {
      val (n, (geom, ((index, pix), ray, l, id))) = elem
      println("lights", n, geom.boundingBox.min, java.net.InetAddress.getLocalHost().getHostAddress())
      (n, ((index, pix), (ray, (geom intersect ray), l, id)))
    })
    val notPartitioned: RDD[((Int, Pixel), (Ray, Option[IntersectData], RTColor, IntersectData))] = withOIDs.values
    val byLight = notPartitioned.map { case ((index, pix), (r, oid, rtCol, id)) => ((index, pix), (r, oid, rtCol, id)) }

    byLight.reduceByKey((t1, t2) => {
      val (r1, oid1, rtCol1, id1) = t1
      val (r2, oid2, rtCol2, id2) = t2
      if (oid1.isEmpty) t2 else t1
    }).filter((elem: ((Int, Pixel), (Ray, Option[IntersectData], RTColor, IntersectData))) => {
      val ((index, pix), (ray, oid, col, id)) = elem
      oid == None
    }).map { case ((index, pix), (ray, oid, col, id)) => (pix, (ray, oid, col, id)) }
  }

  private def generateColors(lightIntersects: RDD[(Pixel, (Ray, Option[IntersectData], RTColor, IntersectData))]): RDD[(Pixel, RTColor)] = {
    lightIntersects.aggregateByKey(RTColor.Black)({ case (color, (r, oid, rtCol, id)) =>
      color + oid.map { nid => if (nid.time < 0 || nid.time > 1) {
              val intensity = (r.dir.normalize dot id.norm).toFloat
              if (intensity < 0) RTColor.Black else rtCol * intensity
            } else RTColor.Black
      }.getOrElse{
        val intensity = (r.dir.normalize dot id.norm).toFloat
        if (intensity < 0) RTColor.Black else rtCol * intensity
      }
    }, _ + _)
  }

  private def combineAndSetColors(colors: RDD[(Pixel, RTColor)], img: RTImage, numRays: Int): Unit = {
    val combinedColors = colors.reduceByKey(_ + _).mapValues(rt => (rt / numRays).copy(a = 1.0)).collect
    for ((pix, c) <- combinedColors.par) {
      img.setColor(pix.x, pix.y, c)
    }
  }
}


