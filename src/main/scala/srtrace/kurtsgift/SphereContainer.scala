package kurtsgift

import swiftvis2.raytrace.{Point, Vect, Sphere}

//Serializable Container for Sphere trait
case class SphereContainer(center: Point, radius: Double) extends Sphere {
  //Never used, so stubbed until needed
  def movedBy(v: Vect): Sphere = {
    ???
  }
}
object SphereContainer {
  def apply(s: Sphere): SphereContainer = {
    new SphereContainer(s.center, s.radius)
  }
}