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
	def randomGeometryArr(rand:scala.util.Random, maxX:Int, minX:Int, maxY:Int, minY:Int, maxZ:Int, minZ:Int, maxRadius:Int, n:Int):Geometry = {
		def randGeometry():Geometry = {
			
			val x = rand.nextInt(maxX - minX) + minX
			val y = rand.nextInt(maxY - minY) + minY
			val z = rand.nextInt(maxZ - minZ) + minZ
			val rad = rand.nextInt(maxRadius)
			val center = new Point(x, y, z)
			new GeomSphere(center, rad, p => RTColor(0xFFFFFF00), p => 0.0)
		}
		var randGeoms:Array[Geometry] = Array.fill(n)(randGeometry)
		new ListScene(randGeoms :_ *)
	}

	def readParticles(): Geometry = {
		//Pulls the geometry data from the supplied file within the given directory. Assigns the color of the spheres to black.
		val carURL = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.6029.bin")
		val carURL2 = new URL("http://www.cs.trinity.edu/~mlewis/Rings/AMNS-Moonlets/Moonlet4/CartAndRad.6028.bin")
  		val particles1 = CartAndRad.readStream(carURL.openStream).map(p => GeomSphere(Point(p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles2 = CartAndRad.readStream(carURL2.openStream).map(p => GeomSphere(Point(p.x, p.y, p.z), p.rad, _ => new RTColor(1, 1, 1, 1), _ => 0.0))
		val particles = particles1 ++ particles2
		val particleSpheres = particles.map(p => new GeomSphere(p.center, p.radius, _ => RTColor.Red, _ => 0))
		new KDTreeGeometry(particleSpheres)
	}

	def standardView(): (Point, Point, Vect, Vect) = {
		(Point(0.0, 0.0, 0.0), Point(-2.0, 2.0, 2.0), Vect(4.0, 0.0, 0.0), Vect(0.0, 0.0, -4.0))
	}

	def ringView1(dist: Double): (Point, Point, Vect, Vect)  = {
		(Point(0.0, 0.0, dist), Point(-dist, dist, 0.0), Vect(2*dist, 0.0, 0.0), Vect(0.0, 2*dist, 0.0))
	}



}