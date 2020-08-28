package srtrace

import java.awt.Graphics
import java.awt.image.BufferedImage

import javax.swing._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._

object Renderer3 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Renderer3").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val size = 600
    //val geom = GeometrySetup.randomGeometryArr(new scala.util.Random(System.currentTimeMillis), 5, -5, 20, 10, 5, -5, 2, 20) //new GeomSphere(Point(1.0, 5.0, 0.0), 1.0, p => RTColor(0xFFFFFF00), p => 0.0)
    //val broadcastGeom = sc.broadcast(geom)
    val light: List[PointLight] = List(new PointLight(RTColor.White, Point(-2.0, 0.0, 2.0)))
    val bimg = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
    for (i <- 0 until size; j <- 0 until size) bimg.setRGB(i, j, 0xFF000000)
    val img = new RTImage {
      def width = bimg.getWidth()

      def height = bimg.getHeight()

      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bimg.setRGB(x, y, color.toARGB)
      }
    }
    val numRays = 1

    def makeNPartitionsRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numPartitions: Int, numRays: Int):
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

    //GeometryRDD


    def makeRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numRays: Int): RDD[((Int, Int), Ray)] = {
      //make rays in a scala collection
      val aspect = img.width.toDouble / img.height
      val rays = for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
        ((i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width)
          + down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
      }
      sc.parallelize(rays)
    }

    def intersectEye(rayGeoms: RDD[(Int, (((Int, Int), Ray), Geometry))]): RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))] = {
      rayGeoms.map(indiv => {
        val (n, (((x, y), ray), geom)) = indiv
        (n, ((x, y), (ray, (geom) intersect ray)))
      })
    }

    def findShortest(iterable:Iterable[(Ray, Option[IntersectData])]):(Ray, Option[IntersectData]) = {
      val iter = iterable.iterator
      var currentMinID:Option[IntersectData] = None
      var currentRay:Option[Ray] = None
      while(iter.hasNext) {
        val (ray, oid):(Ray, Option[IntersectData]) = iter.next
        if(currentRay == None || currentMinID == None) {
          currentRay = Some(ray)
          currentMinID = oid
        } else {
          if(oid != None) {
            if(currentMinID.get.time > oid.get.time) {
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
    def departitionAndFindShortest(bug: RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))]):RDD[((Int, Int), (Ray, Option[IntersectData]))] = {
      val noPartitions = bug.values
      val groupedByPixel:RDD[((Int, Int), Iterable[(Ray, Option[IntersectData])])] = noPartitions.groupByKey()
      val shortestIDs:RDD[((Int, Int), (Ray, Option[IntersectData]))] = groupedByPixel.mapValues(x => {
        findShortest(x)
     })
      shortestIDs
    }

    def explodeLights(rayids: RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))], lights: List[PointLight]): RDD[(Int, ((Int, (Int, Int)), (IntersectData, PointLight)))] = {
      rayids.flatMapValues(rayidi => {
        val rayid = rayidi
        explodeLight(rayid, lights)
      })
    }

    def explodeLight(rayid: ((Int, Int), (Ray, Option[IntersectData])), lights: List[PointLight]): List[((Int, (Int, Int)), (IntersectData, PointLight))] = {
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

    def makeRaysToLights(idLights: RDD[(Int, ((Int, (Int, Int)), (IntersectData, PointLight)))], numPartitions:Int):RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))] = {
      val repart: RDD[(Int, ((Int, (Int, Int)), (IntersectData, PointLight)))] = idLights.values.flatMap(x => {
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
    def checkLightRaysForGeomIntersections(lightRays:RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))], geom:RDD[(Int, Geometry)]):RDD[((Int, Int), (Ray, Option[IntersectData], RTColor, IntersectData))] = {
      val joined: RDD[(Int, (((Int, (Int, Int)), Ray, RTColor, IntersectData), Geometry))] = lightRays.join(geom)
      val withOIDs: RDD[(Int, ((Int, (Int, Int)), (Ray, Option[IntersectData], RTColor, IntersectData)))] = joined.map(elem => {
        val (n, (((index, (x, y)), ray, l, id), geom)) = elem
        (n, ((index, (x, y)), (ray, (geom intersect ray), l, id)))
      })
      val notPartitioned: RDD[((Int, (Int, Int)), (Ray, Option[IntersectData], RTColor, IntersectData))] = withOIDs.values
      val byLight = notPartitioned.map{case ((index, (x, y)), (r, oid, rtCol, id)) => ((index, (x, y)), (r, oid, rtCol, id))}

      byLight.reduceByKey((t1, t2) => {
        val (r1, oid1, rtCol1, id1) = t1
        val (r2, oid2, rtCol2, id2) = t2
        if(oid1.isEmpty) t2 else t1
      }).filter((elem:((Int, (Int, Int)), (Ray, Option[IntersectData], RTColor, IntersectData))) => {
        val ((index, (x, y)), (ray, oid, col, id)) = elem
        oid == None
      }).map{case ((index, (x, y)), (ray, oid, col, id)) => ((x, y), (ray, oid, col, id))}
    }


    def generateColors(bug: RDD[((Int, Int), (Ray, Option[IntersectData], RTColor, IntersectData))]):RDD[((Int, Int), RTColor)] = {
      val groupedByPixel:RDD[((Int, Int), Iterable[(Ray, Option[IntersectData], RTColor, IntersectData)])] = bug.groupByKey()
      /*val shortestID: RDD[((Int, Int), (Ray, Option[IntersectData], RTColor))] = groupedByPixel.map(x => {
        val (pix, iter) = x
        (pix, iter.minBy(y => {
          val (r, oid, rtColor) = y
          oid.get.time
        }))
      })*/
      groupedByPixel.map(elem => {
        val ((x, y), iter:Iterable[(Ray, Option[IntersectData], RTColor, IntersectData)]) = elem
        val color = iter.map(i => {
          val (r, oid:Option[IntersectData], rtCol, id) = i
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

    
    def combineAndSetColors(colors: RDD[((Int, Int), RTColor)], img: RTImage, numRays: Int): Unit = {
      val combinedColors = colors.reduceByKey(_ + _).mapValues(rt => (rt / numRays).copy(a = 1.0)).collect
      for (((x, y), c) <- combinedColors.par) {
        img.setColor(x, y, c)
      }
    }


//    def castRay(ray: Ray, bGeom: Broadcast[Geometry], lights: List[Light], cnt: Int): RTColor = {
//      if (cnt > 5) new RTColor(0, 0, 0, 1)
//      else {
//        val oid = (bGeom.value) intersect ray
//        oid match {
//          case None => RTColor.Black
//          case Some(id) => {
//            val geomSize = id.geom.boundingSphere.radius
//            val lightColors = for (light <- lights) yield light.color(id, bGeom.value)
//            val refColor = if (id.reflect > 0) {
//              val refRay = new Ray(id.point + id.norm * 0.0001 * geomSize, ray.dir - id.norm * 2 * (id.norm dot ray.dir))
//              castRay(refRay, bGeom, lights, cnt + 1)
//            } else new RTColor(0, 0, 0, 1)
//            id.color * (lightColors.foldLeft(new RTColor(0, 0, 0, 1))(_ + _)) + refColor
//          }
//        }
//      }
//    }
    val minX = -10.0
    val maxX = 10.0 //for use in arrGeoms
    val diff = maxX - minX
    val numPartitions = 8
    val interval = diff / numPartitions
    //val arrGeoms:RDD[GeomSphere] = sc.parallelize(GeometrySetup.randomGeometryActualArr(new scala.util.Random(System.currentTimeMillis), maxX, minX,20,10,10,-10,2, 5)) //actual geometries
    //val arrGeoms:RDD[GeomSphere] = sc.parallelize(GeometrySetup.makeTwoSpheresVisuallyIntersecting()) //only for testing visual intersections
    val arrGeoms:RDD[GeomSphere] = sc.parallelize(GeometrySetup.makeTwoSpheresIntersecting()) //only for testing physical intersections
    // println("\n\nARRGEOMS PRINTING NOW")g
    // arrGeoms.collect.foreach(println)
    val keyedGeoms:RDD[(Int, GeomSphere)] = arrGeoms.map(iGeom => ((iGeom.center.x - minX) / diff * numPartitions).toInt -> iGeom).repartition(numPartitions)
    // println("\n\nKEYEDGEOMs PRINTING NOW")
    // keyedGeoms.collect.foreach(println)
    val groupedGeoms:RDD[(Int, Geometry)] = keyedGeoms.groupByKey().map{case (i, spheres) => i -> new KDTreeGeometry(spheres.toSeq)}
    // println("\n\nGROUPEDGEOMS PRINTING NOW")
    // groupedGeoms.collect.foreach(println)
    val (eye, topLeft, right, down) = GeometrySetup.standardView()
    val dupedRays:RDD[(Int, ((Int, Int), Ray))] = makeNPartitionsRays(sc, eye, topLeft, right, down, img, numPartitions, numRays)
    // println("\n\nDUPEDRAYS PRINTING NOW")
    // dupedRays.collect.foreach(println)
    val rayGeoms:RDD[(Int, (((Int, Int), Ray), Geometry))] = dupedRays.join(groupedGeoms)
    // println("\n\nRAYGEOMS PRINTING NOW")
    // rayGeoms.collect.foreach(println)
    val rayoids:RDD[(Int, ((Int, Int), (Ray, Option[IntersectData])))] = intersectEye(rayGeoms)
    // println("\n\nRAYOIDS PRINTING NOW")
    // rayoids.collect.foreach(println)

    val fixBroken:RDD[((Int, Int), (Ray, Option[IntersectData]))] = departitionAndFindShortest(rayoids)
    // Collapse to one intersect per pixel. (Minimum by id.time.)
    // Explode needs to re-distribute. Explode in lights and then in geometry partitions.

    val idLights: RDD[(Int, ((Int, (Int, Int)), (IntersectData, PointLight)))] = explodeLights(rayoids, light)
    // println("\n\nIDLIGHTS PRINTING NOW")
    // idLights.collect.foreach(println)
    //val colorLocations:RDD[((Int, Int), RTColor)] = calcLightColors(idLights, ???)

    val lightColors: RDD[(Int, ((Int, (Int, Int)), Ray, RTColor, IntersectData))] = makeRaysToLights(idLights, numPartitions)

    val idRDD: RDD[((Int, Int), (Ray, Option[IntersectData], RTColor, IntersectData))] = checkLightRaysForGeomIntersections(lightColors, groupedGeoms)

    val colors: RDD[((Int, Int), RTColor)] = generateColors(idRDD)


    // println("\n\nCOLORLOCATIONS PRINTING NOW")
    // colorLocations.collect.foreach(println)



    combineAndSetColors(colors, img, numRays)
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