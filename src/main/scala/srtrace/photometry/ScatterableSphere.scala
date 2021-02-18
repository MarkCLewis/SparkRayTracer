package srtrace.photometry

import swiftvis2.raytrace._

class ScatterableSphere(
    center: Point,
    radius: Double,
    color: Point => RTColor,
    reflect: Point => Double
) extends GeomSphere(center, radius, color, reflect)
    with ScatterableGeometry {

  override def fractionScattered(
      incomingDir: Vect,
      outgoingDir: Vect,
      intersectData: IntersectData
  ): Double = outgoingDir.normalize.dot(intersectData.norm)

}
