package srtrace

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import swiftvis2.raytrace._
import java.awt.image.BufferedImage
import javax.swing._
import java.awt.Graphics

object Renderer2 {
	def main(args: Array[String]) = {
		val conf = new SparkConf().setAppName("Renderer1").setMaster("local[*]")
		val sc = new SparkContext(conf)
  
		sc.setLogLevel("WARN")

		val size = 1000
		val geom = GeometrySetup.randomGeometryArr(new scala.util.Random(System.currentTimeMillis), 10,-10,20,10,10,-10,2,10) //new GeomSphere(Point(1.0, 5.0, 0.0), 1.0, p => RTColor(0xFFFFFF00), p => 0.0)
		val light = List(new PointLight(RTColor.White, Point(-2.0, 0.0, 2.0)))
		val bimg = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
		val img = new RTImage {
			def width = bimg.getWidth()
			def height = bimg.getHeight()
			def setColor(x: Int, y: Int, color: RTColor): Unit = {
				bimg.setRGB(x, y, color.toARGB)
			}
		}
		val numRays = 2
		//val randGeoms = randomGeometryArr(new util.Random(System.currentTimeMillis), 100,0,100,0,100,0,5,100)
		val rays = makeRays(sc, Point(0.0, 0.0, 0.0), Point(-2.0, 2.0, 2.0), Vect(4.0, 0.0, 0.0), Vect(0.0, 0.0, -4.0), img, numRays)
		val colors = transform(rays, geom, light)
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

	

	def makeRays(sc: SparkContext, eye: Point, topLeft: Point, right: Vect, down: Vect, img: RTImage, numRays: Int): 
		RDD[((Int, Int), Ray)] = {
		//make rays in a scala collection
		val aspect = img.width.toDouble/img.height
		val rays = for (i <- 0 until img.width; j <- 0 until img.height; index <- 0 until numRays) yield {
			((i, j), Ray(eye, topLeft + right * (aspect * (i + (if (index > 0) math.random * 0.75 else 0)) / img.width) 
				+ down * (j + (if (index > 0) math.random * 0.75 else 0)) / img.height))
		}
		sc.parallelize(rays)
	}

	def transform(rays: RDD[((Int, Int), Ray)], geom: Geometry, lights: List[Light]):  RDD[((Int, Int), RTColor)] = {
		//rays.mapValues(ray => castRay(ray, geom, lights, 0))
		val geomIDs: RDD[((Int, Int), (Ray, Option[IntersectData]))] = rays.mapValues(ray => (ray, geom intersect ray))
		val lightStuff: RDD[((Int, Int), (Option[IntersectData, Light]))] = geomIds.flatMap((x, y), (ray, oid) => oid match {
			case None => ()
			case Some(id) => {
				lights.map(light => ((x, y), (Some(id, light))))
			}
		})
		val colors:RDD[((Int, Int), RTColor)] = lightStuff.reduceByKey((((x1:Int, y1:Int), oidl1:Option[IntersectData, Light]), ((x2:Int, y2:Int), oidl2:Option[IntersectData, Light])) => )
		colors
	}
	

	// Not in parallel
	def combineAndSetColors(colors: RDD[((Int, Int), RTColor)], img: RTImage, numRays: Int): Unit = {
		val combinedColors = colors.reduceByKey( _ + _ ).mapValues(_/numRays).collect
		for( ((x,y) ,c) <- combinedColors) {
			img.setColor(x,y,c)
		}
	}


	def castRay(ray: Ray, geom: Geometry, lights: List[Light], cnt: Int): RTColor = {
    if (cnt > 5) new RTColor(0, 0, 0, 1)
    else {
      val oid = geom intersect ray
      oid match {
        case None => RTColor.Black
        case Some(id) => {
			val geomSize = id.geom.boundingSphere.radius
			val lightColors = for (light <- lights) yield light.color(id, geom)
			val refColor = if (id.reflect > 0) {
            	val refRay = new Ray(id.point + id.norm * 0.0001 * geomSize, ray.dir - id.norm * 2 * (id.norm dot ray.dir))
            	castRay(refRay, geom, lights, cnt + 1)
          } else new RTColor(0, 0, 0, 1)
          	id.color * (lightColors.foldLeft(new RTColor(0, 0, 0, 1))(_ + _)) + refColor
        }
      }
    }
  }
	
}

