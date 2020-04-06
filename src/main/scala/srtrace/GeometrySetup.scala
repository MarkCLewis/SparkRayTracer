package srtrace

import swiftvis2.raytrace._
import java.awt.Graphics
import data.CartAndRad
import java.awt.image.BufferedImage
import javax.swing._
import java.net.URL




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
			val x = rand.nextDouble *(maxX - minX) + minX
			val y = rand.nextDouble * (maxY - minY) + minY
			val z = rand.nextDouble * (maxZ - minZ) + minZ
			val rad = rand.nextDouble * (maxRadius)
			val center = new Point(x, y, z)
			new GeomSphere(center, rad, p => RTColor(0xFFFFFF00), p => 0.0)
		}
		Array.fill(n)(randGeometry)
	}
	

	def readParticles(): Geometry = {
		//Pulls the geometry data from the supplied file within the given directory. Assigns the color of the spheres to black.
  		val carURL = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.6029.bin")
  		val particles = CartAndRad.readStream(carURL.openStream).map(p => GeomSphere(Point(p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		
		val particleSpheres = particles.map(p => new GeomSphere(p.center, p.radius, _ => RTColor.Red, _ => 0))
		new KDTreeGeometry(particleSpheres)
	}

	def standardView(): (Point, Point, Vect, Vect) = {
		(Point(0.0, 0.0, 0.0), Point(-2.0, 2.0, 2.0), Vect(4.0, 0.0, 0.0), Vect(0.0, 0.0, -4.0))
	}

	def ringView1(): (Point, Point, Vect, Vect)  = {
	???
	}



}