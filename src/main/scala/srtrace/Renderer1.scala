package srtrace

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._
import java.awt.image.BufferedImage

object Renderer1 {
	def main(args: Array[String]) = {
		val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
		val sc = new SparkContext(conf)
  
		sc.setLogLevel("WARN")

		val geom = new GeomSphere(Point(1.0, 1.0, 0.0), 1.0, p => RTColor.Red, p => 0.0)
		val light = List(new AmbientLight(RTColor.White))
		val bimg = new BufferedImage(100, 100, BufferedImage.TYPE_INT_ARGB)
		val img = new RTImage {
			def width = bimg.getWidth()
			def height = bimg.getHeight()
			def setColor(x: Int, y: Int, color: RTColor): Unit = {
				bimg.setRGB(x, y, color.toARGB)
			}
		}

		val rays = makeRays(sc, Point(0.0, 0.0, 0.0), Point(-2.0, 0.0, 2.0), Vect(4.0, 0.0, 0.0), Vect(0.0, 0.0, -4.0), img, 10)
	
		sc.stop()
	}


		def makeRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numRays: Int): RDD[((Int, Int), Ray)] = {
			//make rays in a scala collection
			val aspect = img.width.toDouble/img.height
			val rays = for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
        	((i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width) 
										+ down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
    	}
//			sc.parallelize(rays)
		???
		}

		def transform(rays: RDD[((Int, Int), Ray)], geom: Geometry, lights: List[Light]):  RDD[((Int, Int), RTColor)] = {
		rays.mapValues(ray => RayTrace.castRay(ray, geom, lights, 0))
	}
	

	// Not in parallel
	def combineAndSetColors(colors: RDD[((Int, Int), RTColor)], img: RTImage, numRays: Int): Unit = {
		val combinedColors = colors.reduceByKey( _ + _ ).mapValues(_/numRays).collect
		for( ((x,y) ,c) <- combinedColors) {
			img.setColor(x,y,c)
		}
	}

	
}

