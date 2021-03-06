package srtrace

import java.net.URL

import scala.util.Random

import data.CartAndRad
import swiftvis2.raytrace._
import srtrace.photometry.ScatterableSphere




//Need to load geometry from file. Copy file stuff from akka stuff on github. 
//Broadcast in Render1


  


object GeometrySetup {
    /*
		randomGeometryArr takes the random generator, then max and min values for x, y, and z, as well as a max radius, and
        the number of geomSpheres you want. It currently returns a ListScene of geometry, which is a subclass of geometry and can
        thus be used in our ray tracing functions.
	*/
	def randomGeometryArr(rand:scala.util.Random, maxX:Double, minX:Double, maxY:Double, minY:Double, maxZ:Double, minZ:Double, maxRadius:Double, n:Int):Geometry = {
		var randGeoms:Array[GeomSphere] = randomGeometryActualArr(rand, maxX, minX, maxY, minY, maxZ, minZ, maxRadius, n)
		new ListScene(randGeoms :_ *)
	}

	def randomGeometryActualArr(rand:scala.util.Random, maxX:Double, minX:Double, maxY:Double, minY:Double, maxZ:Double, minZ:Double, maxRadius:Double, n:Int):Array[GeomSphere] = {
		def randGeometry():GeomSphere = {
			val x = rand.nextDouble * (maxX - minX) + minX
			val y = rand.nextDouble * (maxY - minY) + minY
			val z = rand.nextDouble * (maxZ - minZ) + minZ
			val rad = 3 //rand.nextDouble * (maxRadius)
			val center = new Point(x, y, z)
			new GeomSphere(center, rad, p => RTColor(0xFFFFFF00), p => 0.0)
		}
		Array.fill(n)(randGeometry)
	}

	//for photometric geom we need an RDD[(Int, KDTreeGeometry[BoundingBox])]
  	def smallPhoGeom():Array[(Int, KDTreeGeometry[BoundingBox])] = {
		Array((1, new KDTreeGeometry[BoundingBox](randomGeometryActualArr(new Random(), 10, -10, 10, 15, 10, -10, 4, 20), 5, BoxBoundsBuilder)))
  	}

	def readParticles(): Geometry = {
		//Pulls the geometry data from the supplied file within the given directory. Assigns the color of the spheres to black.
		val carURL = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5000.bin")
		val carURL2 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5001.bin")
		val carURL3 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5002.bin")
		val carURL4 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5003.bin")
		val carURL5 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5004.bin")
		val carURL6 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5005.bin")
		val carURL7 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5006.bin")
		// val carURL8 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5007.bin")
		// val carURL9 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5008.bin")
		// val carURL10 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5009.bin")
		// val carURL11 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.5010.bin")
		//val carURL12 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.6029.bin")
		//val carURL13 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.6028.bin")

		val particles1 = CartAndRad.readStream(carURL.openStream).map(p => GeomSphere(Point(-6*2.0e-5-p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles2 = CartAndRad.readStream(carURL2.openStream).map(p => GeomSphere(Point(-5*2.0e-5-p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles3 = CartAndRad.readStream(carURL3.openStream).map(p => GeomSphere(Point(-4*2.0e-5-p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles4 = CartAndRad.readStream(carURL4.openStream).map(p => GeomSphere(Point(-3*2.0e-5-p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles5 = CartAndRad.readStream(carURL5.openStream).map(p => GeomSphere(Point(-2*2.0e-5-p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles6 = CartAndRad.readStream(carURL6.openStream).map(p => GeomSphere(Point(-2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles7 = CartAndRad.readStream(carURL7.openStream).map(p => GeomSphere(Point(p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		// val particles8 = CartAndRad.readStream(carURL8.openStream).map(p => GeomSphere(Point(2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		// val particles9 = CartAndRad.readStream(carURL9.openStream).map(p => GeomSphere(Point(2*2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		// val particles10 = CartAndRad.readStream(carURL10.openStream).map(p => GeomSphere(Point(3*2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		// val particles11 = CartAndRad.readStream(carURL11.openStream).map(p => GeomSphere(Point(4*2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		//val particles12 = CartAndRad.readStream(carURL12.openStream).map(p => GeomSphere(Point(5*2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		//val particles13 = CartAndRad.readStream(carURL13.openStream).map(p => GeomSphere(Point(6*2.0e-5+p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles = particles1 ++ particles2 ++ particles3 ++ particles4 ++ particles5 ++ particles6 ++ particles7 //++ particles8 ++ particles9 ++ particles10 ++ particles11 //++ particles12 ++ particles13
		val particleSpheres = particles.map(p => new GeomSphere(p.center, p.radius, _ => RTColor.Red, _ => 0))
		new KDTreeGeometry(particleSpheres)
	}

	def readRingWithOffsetSpheres(step: Int, xoff: Double, yoff: Double): Seq[Geometry] = {
		val carURL = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad." + step.toString + ".bin")
		CartAndRad.readStream(carURL.openStream).map(p => ScatterableSphere(GeomSphere(Point(p.x + xoff, p.y + yoff, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0)))
	}

	def readRingWithOffset(step: Int, xoff: Double, yoff: Double): KDTreeGeometry[BoundingSphere] = {
		lazy val particleSpheres = readRingWithOffsetSpheres(step, xoff, yoff)
		new KDTreeGeometry[BoundingSphere](particleSpheres)
	}
	def readRingWithOffsetBox(step: Int, xoff: Double, yoff: Double): KDTreeGeometry[BoundingBox] = {
		val particleSpheres = readRingWithOffsetSpheres(step, xoff, yoff)
		new KDTreeGeometry[BoundingBox](particleSpheres, 5, BoxBoundsBuilder)
	}

	def standardView(): (Point, Point, Vect, Vect) = {
		(Point(0.0, 0.0, 0.0), Point(-2.0, 2.0, 2.0), Vect(4.0, 0.0, 0.0), Vect(0.0, 0.0, -4.0))
		//eye, topLeft, right, down
	}
	def topView(distMult: Double): (Point, Point, Vect, Vect) = {
		//eye,                          topLeft,                                right,                down
		(Point(0.0, 0.0, distMult*1e-5), Point(-1e-5, 1e-5, (distMult-1)*1e-5), Vect(2 * 1e-5, 0, 0), Vect(0, -2 * 1e-5, 0))
	}

	def positiveYView(): (Point, Point, Vect, Vect) = {
		val cellWidth = 1e-5
  		val distanceUp = 1e-5
  		val viewSize = 1e-5
  		val numSims = 6
  		val firstXOffset = cellWidth * (numSims - 1)
		(Point(0, -firstXOffset-2*cellWidth, distanceUp), Point(-viewSize, -firstXOffset-2*cellWidth+viewSize, distanceUp + viewSize), Vect(2 * viewSize, 0, 0), Vect(0, 0, -2 * viewSize))
	}


	def makeTwoSpheresIntersecting():Array[GeomSphere] = {
		val arr:Array[GeomSphere] = Array(new GeomSphere(Point(1.0, 6.0, 1.0), 3.0, p => RTColor(0xFFFFFF00), p=> 0.0),
			new GeomSphere(Point(2.0, 6.0, 1.0), 3.0, p => RTColor(0xFFFFFF00), p=> 0.0))
		arr
	}
	def makeTwoSpheresVisuallyIntersecting():Array[GeomSphere] = {
		val arr:Array[GeomSphere] = Array(new GeomSphere(Point(1.0, 4.0, 1.0), 3.0, p => RTColor(0xFFFFFF00), p=> 0.0),
			new GeomSphere(Point(3.0, 8.0, 1.0), 3.0, p => RTColor(0xFFFFFF00), p=> 0.0))
		arr
	}
	def ringView1(dist: Double): (Point, Point, Vect, Vect)  = {
		(Point(0.0, 0.0, dist), Point(-dist, dist, 0.0), Vect(2*dist, 0.0, 0.0), Vect(0.0, 2*dist, 0.0))
	}

	def fixedSpheres(): Array[GeomSphere] = {
		// Put a fixed scene in where we know what it looks like.
		Array.empty
	}



}