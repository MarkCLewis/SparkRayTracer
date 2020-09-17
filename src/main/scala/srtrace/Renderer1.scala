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
//	def main(args: Array[String]) = {
//		val conf = new SparkConf().setAppName("Renderer1").setMaster("local[*]")
//		val sc = new SparkContext(conf)
//
//		sc.setLogLevel("WARN")
//
//		val size = 1000
//		val numOfSpheres = 500//3900 works
//		val geom: Geometry = GeometrySetup.randomGeometryArr(new scala.util.Random(System.currentTimeMillis), 10,-10,20,10,10,-10,2,numOfSpheres) //new GeomSphere(Point(1.0, 5.0, 0.0), 1.0, p => RTColor(0xFFFFFF00), p => 0.0)
		//val geom2 = GeometrySetup.readParticles()

//		val broadcastVar = sc.broadcast(geom)
//		val light = List(new PointLight(RTColor.White, Point(-2.0, 0.0, 2.0)))
//		val bimg = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
//		val img = new RTImage {
//			def width = bimg.getWidth()
//			def height = bimg.getHeight()
//			def setColor(x: Int, y: Int, color: RTColor): Unit = {
//				bimg.setRGB(x, y, color.toARGB)
//			}
//		}
//		val numRays = 2
		//val randGeoms = randomGeometryArr(new util.Random(System.currentTimeMillis), 100,0,100,0,100,0,5,100)
		//val (eye, topLeft, right, down) = GeometrySetup.standardView()
//		val rays = makeRays(sc, eye, topLeft, right, down, img, numRays)
//		val colors = transform(rays, broadcastVar.value, light)
//		combineAndSetColors(colors, img, numRays)

		

//		val frame = new JFrame {
//			override def paint(g: Graphics): Unit = {
//				g.drawImage(bimg, 0, 0, null)
//			}
//		}
//		frame.setSize(size, size)
//		frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
//		frame.setVisible(true)
//		sc.stop()
//	}

	

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

