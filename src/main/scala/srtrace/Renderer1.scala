package srtrace

import java.awt.image.BufferedImage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._

object Renderer1 {
	def render(sc:SparkContext, geom: Geometry, light: List[PointLight], bimg:BufferedImage, view: (Point, Point, Vect, Vect), size: Int, numRays: Int = 1, numPartitions: Int = 8):Unit = {
		val broadcastVar = sc.broadcast(geom)
		val img = new RTImage {
			def width = bimg.getWidth()
			def height = bimg.getHeight()
			def setColor(x: Int, y: Int, color: RTColor): Unit = {
				bimg.setRGB(x, y, color.toARGB)
			}
		}
		val (eye, topLeft, right, down) = view
		val rays = makeRays(sc, eye, topLeft, right, down, img, numRays)
		val colors = transform(rays, broadcastVar.value, light)
		combineAndSetColors(colors, img, numRays)
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

	def transform(rays: RDD[(Pixel, Ray)], geom: Geometry, lights: List[Light]):  RDD[(Pixel, RTColor)] = {
		rays.mapValues(ray => RayTrace.castRay(ray, geom, lights, 0))
	}
	

	// Not in parallel
	def combineAndSetColors(colors: RDD[(Pixel, RTColor)], img: RTImage, numRays: Int): Unit = {
		val combinedColors = colors.reduceByKey( _ + _ ).mapValues(_/numRays).collect
		for( (Pixel(x,y) ,c) <- combinedColors) {
			img.setColor(x,y,c)
		}
	}

	
}

