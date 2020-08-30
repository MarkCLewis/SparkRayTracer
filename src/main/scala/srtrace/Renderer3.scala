package srtrace

import java.awt.image.BufferedImage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._

// TODO: Put in some meaningful case classes so we don't have tuples everywhere.

object Renderer3 {


  def render(sc: SparkContext, geom: RDD[GeomSphere], light: List[PointLight], bImg: BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays: Int = 1, numPartitions: Int = 8, minX: Double, maxX: Double): Unit = {

    val img = new RTImage {
      def width = bImg.getWidth()

      def height = bImg.getHeight()

      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bImg.setRGB(x, y, color.toARGB)
      }
    }
    for (i <- 0 until size; j <- 0 until size) bImg.setRGB(i, j, 0xFF000000)
    val diff = maxX - minX


    val keyedGeoms: RDD[(Int, GeomSphere)] = geom.map(iGeom => ((iGeom.center.x - minX) / diff * numPartitions).toInt -> iGeom).repartition(numPartitions)
    val groupedGeoms: RDD[(Int, Geometry)] = keyedGeoms.groupByKey().map { case (i, spheres) => i -> new KDTreeGeometry(spheres.toSeq) }
    val dupedRays: RDD[(Int, ((Int, Int), Ray))] = makeNPartitionsRays(sc, view._1, view._2, view._3, view._4, img, numPartitions, numRays)
    val rayGeoms: RDD[(Int, (((Int, Int), Ray), Geometry))] = dupedRays.join(groupedGeoms)
    val rayoids: RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))] = intersectEye(rayGeoms)
    val fixBroken: RDD[((Int, Int), (Ray, Option[IntersectData]))] = departitionAndFindShortest(rayoids)
    val idLights: RDD[((Int, (Int, Int)), (IntersectData, PointLight))] = explodeLights(fixBroken, light)
    val lightColors: RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))] = makeRaysToLights(idLights, numPartitions)
    val idRDD: RDD[((Int, Int), (Ray, Option[IntersectData], RTColor, IntersectData))] = checkLightRaysForGeomIntersections(lightColors, groupedGeoms)
    val colors: RDD[((Int, Int), RTColor)] = generateColors(idRDD)


    combineAndSetColors(colors, img, numRays)
    sc.stop()
  }

  private def makeNPartitionsRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numPartitions: Int, numRays: Int):
  RDD[(Int, ((Int, Int), Ray))] = {
    //make rays in a scala collection
    val aspect = img.width.toDouble / img.height
    val rays = (for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
      ((i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width)
        + down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
    }).flatMap(x => {
      for (i <- 0 until numPartitions) yield {
        i -> x
      }
    })
    sc.parallelize(rays)
  }
  
  private def intersectEye(rayGeoms: RDD[(Int, (((Int, Int), Ray), Geometry))]): RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))] = {
    rayGeoms.map(indiv => {
      val (n, (((x, y), ray), geom)) = indiv
      (n, ((x, y), (ray, (geom) intersect ray)))
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
  private def departitionAndFindShortest(bug: RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))]): RDD[((Int, Int), (Ray, Option[IntersectData]))] = {
    val noPartitions = bug.values
    val groupedByPixel: RDD[((Int, Int), Iterable[(Ray, Option[IntersectData])])] = noPartitions.groupByKey()
    val shortestIDs: RDD[((Int, Int), (Ray, Option[IntersectData]))] = groupedByPixel.mapValues(x => {
      findShortest(x)
    })
    shortestIDs
  }

  private def explodeLights(rayids: RDD[((Int, Int), (Ray, Option[IntersectData]))], lights: List[PointLight]): RDD[((Int, (Int, Int)), (IntersectData, PointLight))] = {
    rayids.flatMap(rayidi => {
      val rayid = rayidi
      explodeLight(rayid, lights)
    })
  }

  private def explodeLight(rayid: ((Int, Int), (Ray, Option[IntersectData])), lights: List[PointLight]): Seq[((Int, (Int, Int)), (IntersectData, PointLight))] = {
    val ((x: Int, y: Int), (ray: Ray, oid: Option[IntersectData])) = rayid
    oid match {
      case None => List[((Int, (Int, Int)), (IntersectData, PointLight))]()
      case Some(id: IntersectData) => {
        val ret: List[((Int, (Int, Int)), (IntersectData, PointLight))] = lights.zipWithIndex.map { case (light, index) =>
          ((index, (x, y)), ((id, light)))
        }
        ret
      }
    }
  }

  private def makeRaysToLights(idLights: RDD[((Int, (Int, Int)), (IntersectData, PointLight))], numPartitions: Int): RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))] = {
    val repart: RDD[(Int, ((Int, (Int, Int)), (IntersectData, PointLight)))] = idLights.flatMap(x => {
      for (i <- 0 until numPartitions) yield {
        (i, x)
      }
    })
    val lightRays: RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))] = repart.map(elem => {
      val (n, pixIDLights) = elem
      val ((index, (x, y)), (id, pl)) = pixIDLights
      val loc = id.point
      val li = pl.point
      val c = pl.col
      (n, ((index, (x, y)), Ray(loc + id.norm * 0.0001 * id.geom.boundingSphere.radius, li), c, id))

    })
    lightRays
  }

  //TODO: we need to verify that we never pass around the entire geometry.
  private def checkLightRaysForGeomIntersections(lightRays: RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))], geom: RDD[(Int, Geometry)]): RDD[((Int, Int), (Ray, Option[IntersectData], RTColor, IntersectData))] = {
    val joined: RDD[(Int, (((Int, (Int, Int)), Ray, RTColor, IntersectData), Geometry))] = lightRays.join(geom)
    val withOIDs: RDD[(Int, ((Int, (Int, Int)), (Ray, Option[IntersectData], RTColor, IntersectData)))] = joined.map(elem => {
      val (n, (((index, (x, y)), ray, l, id), geom)) = elem
      (n, ((index, (x, y)), (ray, (geom intersect ray), l, id)))
    })
    val notPartitioned: RDD[((Int, (Int, Int)), (Ray, Option[IntersectData], RTColor, IntersectData))] = withOIDs.values
    val byLight = notPartitioned.map { case ((index, (x, y)), (r, oid, rtCol, id)) => ((index, (x, y)), (r, oid, rtCol, id)) }

    byLight.reduceByKey((t1, t2) => {
      val (r1, oid1, rtCol1, id1) = t1
      val (r2, oid2, rtCol2, id2) = t2
      if (oid1.isEmpty) t2 else t1
    }).filter((elem: ((Int, (Int, Int)), (Ray, Option[IntersectData], RTColor, IntersectData))) => {
      val ((index, (x, y)), (ray, oid, col, id)) = elem
      oid == None
    }).map { case ((index, (x, y)), (ray, oid, col, id)) => ((x, y), (ray, oid, col, id)) }
  }


  private def generateColors(bug: RDD[((Int, Int), (Ray, Option[IntersectData], RTColor, IntersectData))]): RDD[((Int, Int), RTColor)] = {
    val groupedByPixel: RDD[((Int, Int), Iterable[(Ray, Option[IntersectData], RTColor, IntersectData)])] = bug.groupByKey()
    groupedByPixel.map(elem => {
      val ((x, y), iter: Iterable[(Ray, Option[IntersectData], RTColor, IntersectData)]) = elem
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
      ((x, y), color)
    })
  }


  private def combineAndSetColors(colors: RDD[((Int, Int), RTColor)], img: RTImage, numRays: Int): Unit = {
    val combinedColors = colors.reduceByKey(_ + _).mapValues(rt => (rt / numRays).copy(a = 1.0)).collect
    for (((x, y), c) <- combinedColors.par) {
      img.setColor(x, y, c)
    }
  }
}


