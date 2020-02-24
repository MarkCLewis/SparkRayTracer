package srtrace

import swiftvis2.raytrace._

object GeometrySetup {
    /*
		randomGeometryArr takes the random generator, then max and min values for x, y, and z, as well as a max radius, and
		the number of geomSpheres you want. It returns some an array of N randomGeomSpheres
	*/
	def randomGeometryArr(rand:util.Random, maxX:Int, minX:Int, maxY:Int, minY:Int, maxZ:Int, minZ:Int, maxRadius:Int, n:Int):Geometry = {
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
}