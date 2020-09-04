package srtrace

import java.awt.image.BufferedImage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._

// TODO: Put in some meaningful case classes so we don't have tuples everywhere.

object Renderer3 {

  case class Pixel(x:Int, y:Int)
  def render(sc: SparkContext, groupedGeoms: RDD[(Int, KDTreeGeometry[BoundingSphere])], light: List[PointLight], bImg: BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays: Int = 1, numPartitions: Int): Unit = {

    val img = new RTImage {
      def width = bImg.getWidth()

      def height = bImg.getHeight()

      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bImg.setRGB(x, y, color.toARGB)
      }
    }
    for (i <- 0 until size; j <- 0 until size) bImg.setRGB(i, j, 0xFF000000)

    val dupedRays: RDD[(Int, (Pixel, Ray))] = makeNPartitionsRays(sc, view._1, view._2, view._3, view._4, img, numPartitions, numRays)
    val rayGeoms: RDD[(Int, ((Pixel, Ray), KDTreeGeometry[BoundingSphere]))] = dupedRays.join(groupedGeoms)
    val rayoids: RDD[(Int, (Pixel, (Ray, Option[IntersectData])))] = intersectEye(rayGeoms)
    val fixBroken: RDD[(Pixel, (Ray, Option[IntersectData]))] = departitionAndFindShortest(rayoids)
    val idLights: RDD[((Int, Pixel), (IntersectData, PointLight))] = explodeLights(fixBroken, light)
    val lightColors: RDD[(Int, ((Int, Pixel), Ray, RTColor, IntersectData))] = makeRaysToLights(idLights, numPartitions)
    val idRDD: RDD[(Pixel, (Ray, Option[IntersectData], RTColor, IntersectData))] = checkLightRaysForGeomIntersections(lightColors, groupedGeoms)
    val colors: RDD[(Pixel, RTColor)] = generateColors(idRDD)


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
    sc.parallelize(rays)
  }
  
  private def intersectEye(rayGeoms: RDD[(Int, ((Pixel, Ray), KDTreeGeometry[BoundingSphere]))]): RDD[(Int, (Pixel, (Ray, Option[IntersectData])))] = {
    rayGeoms.map(indiv => {
      val (n, ((pix, ray), geom)) = indiv
      (n, (pix, (ray, (geom) intersect ray)))
    })
  }

  private def findShortest(iterable: Iterable[(Ray, Option[IntersectData])]): (Ray, Option[IntersectData]) = {
    val iter = iterable.iterator
    var currentMinID: Option[IntersectData] = None
    var currentRay: Option[Ray] = None
    while (iter.hasNext) {
      val (ray, oid): (Ray, Option[IntersectData]) = iter.next
      if (currentRay.isEmpty || currentMinID.isEmpty) {
        currentRay = Some(ray)
        currentMinID = oid
      } else {
        if (oid.isDefined) {
          if (currentMinID.get.time > oid.get.time) {
            currentRay = Some(ray)
            currentMinID = oid
          }
        }
      }

    }
    (currentRay.get, currentMinID)
  }

  // Collapse to one intersect per pixel. (Minimum by id.time.)
  // Explode needs to re-distribute. Explode in lights and then in geometry partitions.
  private def departitionAndFindShortest(bug: RDD[(Int, (Pixel, (Ray, Option[IntersectData])))]): RDD[(Pixel, (Ray, Option[IntersectData]))] = {
    val noPartitions = bug.values
    val groupedByPixel: RDD[(Pixel, Iterable[(Ray, Option[IntersectData])])] = noPartitions.groupByKey()
    val shortestIDs: RDD[(Pixel, (Ray, Option[IntersectData]))] = groupedByPixel.mapValues(x => {
      findShortest(x)
    })
    shortestIDs
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
    })
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

  //TODO: we need to verify that we never pass around the entire geometry.
  private def checkLightRaysForGeomIntersections(lightRays: RDD[(Int, ((Int, Pixel), Ray, RTColor, IntersectData))], geom: RDD[(Int, KDTreeGeometry[BoundingSphere])]): RDD[(Pixel, (Ray, Option[IntersectData], RTColor, IntersectData))] = {
    val joined: RDD[(Int, (((Int, Pixel), Ray, RTColor, IntersectData), KDTreeGeometry[BoundingSphere]))] = lightRays.join(geom)
    val withOIDs: RDD[(Int, ((Int, Pixel), (Ray, Option[IntersectData], RTColor, IntersectData)))] = joined.map(elem => {
      val (n, (((index, pix), ray, l, id), geom)) = elem
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


  private def generateColors(bug: RDD[(Pixel, (Ray, Option[IntersectData], RTColor, IntersectData))]): RDD[(Pixel, RTColor)] = {
    val groupedByPixel: RDD[(Pixel, Iterable[(Ray, Option[IntersectData], RTColor, IntersectData)])] = bug.groupByKey()
    groupedByPixel.map(elem => {
      val (pix, iter: Iterable[(Ray, Option[IntersectData], RTColor, IntersectData)]) = elem
      val color = iter.map(i => {
        val (r, oid: Option[IntersectData], rtCol, id) = i
        val outRay: Ray = r
        oid match {
          case None => {
            val intensity = (outRay.dir.normalize dot id.norm).toFloat
            if (intensity < 0) new RTColor(0, 0, 0, 1) else rtCol * intensity;
          }
          case Some(nid) => {
            if (nid.time < 0 || nid.time > 1) {
              val intensity = (outRay.dir.normalize dot id.norm).toFloat
              if (intensity < 0) new RTColor(0, 0, 0, 1) else rtCol * intensity;
            } else {
              new RTColor(0, 0, 0, 1)
            }
          }
        }
      }).reduceLeft(_ + _)
      /*
      val (n, (r, oid, rtColor)) = x
      (n, rtColor)
      */
      (pix, color)
    })
  }


  private def combineAndSetColors(colors: RDD[(Pixel, RTColor)], img: RTImage, numRays: Int): Unit = {
    val combinedColors = colors.reduceByKey(_ + _).mapValues(rt => (rt / numRays).copy(a = 1.0)).collect
    for ((pix, c) <- combinedColors.par) {
      img.setColor(pix.x, pix.y, c)
    }
  }
}


