package kurtsgift


import swiftvis2.raytrace.{Point, Vect, RTColor, GeomSphere, Sphere, Box, Geometry, IntersectData}

//Serializable Container for IntersectData, with JSON tags for Jackson
case class IntersectContainer(time: Double, point: Point, norm: Vect, color: RTColor, reflect: Double, geom: Geometry) 
      
object IntersectContainer {
    //Apply for easy conversion from IntersectData to IntersectContainer
    def apply(id: IntersectData): IntersectContainer = {
        val ic = new IntersectContainer(id.time, id.point, id.norm, id.color, id.reflect, id.geom)
        ic
    }
}