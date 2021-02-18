package srtrace.photometry

import swiftvis2.raytrace._

trait ScatterableGeometry extends Geometry {
  def fractionScattered(incomingDir: Vect, outgoingDir: Vect, intersectData: IntersectData): Double
}
