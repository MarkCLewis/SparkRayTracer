package srtrace

import java.awt.Graphics
import java.awt.image.BufferedImage
import javax.swing._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import swiftvis2.raytrace._

object Renderer2 {
	def render(sc:SparkContext, geom:Geometry, light:List[PointLight], bimg:BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays: Int = 1, numPartitions: Int = 8):Unit = {
		val broadcastGeom = sc.broadcast(geom)
		for(i <- 0 until size; j <- 0 until size) bimg.setRGB(i, j, 0xFF000000)
		val img = new RTImage {
			def width = bimg.getWidth()
			def height = bimg.getHeight()
			def setColor(x: Int, y: Int, color: RTColor): Unit = {
				bimg.setRGB(x, y, color.toARGB)
			}
		}
		val (eye, topLeft, right, down) = view
		val start = System.nanoTime()
		val rays:RDD[(Pixel, Ray)] = makeRays(sc, eye, topLeft, right, down, img, numRays)
		val rays2:RDD[(Pixel, (Ray, Option[IntersectData]))] = intersectEye(rays, broadcastGeom)
		val rays3:RDD[(Pixel, (IntersectData, PointLight))] = explodeLights(rays2, light)
		val rays4:RDD[(Pixel, RTColor)] = calcLightColors(rays3, broadcastGeom)
		combineAndSetColors(rays4, img, numRays)
		println(s"Seconds taken: ${(System.nanoTime()-start)/1e9}")
	}

	def makeRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numRays: Int): 
		RDD[(Pixel, Ray)] = {
		//make rays in a scala collection
		val aspect = img.width.toDouble/img.height
		val rays = for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
			(Pixel(i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width) 
				+ down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
		}
		sc.parallelize(rays)
	}
	def intersectEye(rays: RDD[(Pixel, Ray)], bGeom: Broadcast[Geometry]):RDD[(Pixel, (Ray, Option[IntersectData]))] = {
		rays.mapValues(ray => (ray, (bGeom.value) intersect ray))
	}
	def explodeLights(rayids: RDD[(Pixel, (Ray, Option[IntersectData]))], lights:List[PointLight]): RDD[(Pixel, (IntersectData, PointLight))] = {
		rayids.flatMap(rayid => {
			explodeLight(rayid, lights)
		})
	}
	def explodeLight(rayid:(Pixel, (Ray, Option[IntersectData])), lights:List[PointLight]): List[(Pixel, (IntersectData, PointLight))] = {
		val (Pixel(x, y), (ray:Ray, oid:Option[IntersectData])) = rayid
		oid match {
				case None => List[(Pixel, (IntersectData, PointLight))]()
				case Some(id:IntersectData) => {
					val ret:List[(Pixel, (IntersectData, PointLight))] = lights.map(light => {
						(Pixel(x, y), ((id, light)))
					})
					ret
				}
			}
	}
	def calcLightColors(idColors: RDD[(Pixel, (IntersectData, PointLight))], bGeom:Broadcast[Geometry]): RDD[(Pixel, RTColor)] = {
		idColors.aggregateByKey(RTColor(0, 0, 0, 1))({ case (agg, (idata, light)) => agg + light.color(idata, bGeom.value)}, _ + _)
	// 	val groupedByLocation:RDD[(Pixel, Iterable[(IntersectData, PointLight)])] = idColors.groupByKey()
	// 	val mapped:RDD[(Pixel, RTColor)] = groupedByLocation.mapValues(findLightRayValues(_, bGeom))
	// 	mapped
	// }
	// def findLightRayValues(lightids: Iterable[(IntersectData, PointLight)], bGeom:Broadcast[Geometry]): RTColor = {
	// 	var startColor = RTColor(0, 0, 0, 1)
	// 	for((id:IntersectData, light:PointLight) <- lightids) {
	// 		val outRay = Ray(id.point + id.norm * 0.0001 * id.geom.boundingSphere.radius, light.point)
	// 		val oid = bGeom.value.intersect(outRay)
	// 		oid match {
	// 			case None => startColor = startColor + light.color(id, bGeom.value)
	// 			case Some(lid) => startColor = startColor + RTColor(0, 0, 0, 1)
	// 		}
	// 	}
	// 	startColor
	}

	def combineAndSetColors(colors: RDD[(Pixel, RTColor)], img: RTImage, numRays: Int): Unit = {
		val combinedColors = colors.reduceByKey( _ + _ ).mapValues(rt => (rt/numRays).copy(a=1.0)).collect
		for( (Pixel(x,y) ,c) <- combinedColors.par) {
			img.setColor(x,y,c)
		}
	}

	def castRay(ray: Ray, bGeom: Broadcast[Geometry], lights: List[Light], cnt: Int): RTColor = {
    if (cnt > 5) new RTColor(0, 0, 0, 1)
    else {
      val oid = (bGeom.value) intersect ray
      oid match {
        case None => RTColor.Black
        case Some(id) => {
			val geomSize = id.geom.boundingSphere.radius
			val lightColors = for (light <- lights) yield light.color(id, bGeom.value)
			val refColor = if (id.reflect > 0) {
            	val refRay = new Ray(id.point + id.norm * 0.0001 * geomSize, ray.dir - id.norm * 2 * (id.norm dot ray.dir))
            	castRay(refRay, bGeom, lights, cnt + 1)
          } else new RTColor(0, 0, 0, 1)
          	id.color * (lightColors.foldLeft(new RTColor(0, 0, 0, 1))(_ + _)) + refColor
        }
      }
    }
  }
}

