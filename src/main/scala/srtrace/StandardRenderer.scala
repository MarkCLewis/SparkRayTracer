package srtrace

import swiftvis2.raytrace._
import java.awt.image.BufferedImage
import javax.swing.JFrame
import javax.swing.WindowConstants
import java.awt.Graphics

object StandardRenderer {
  def main(args: Array[String]): Unit = {
    val numSims = 21
    val view = GeometrySetup.topView(numSims)
    val size = 1000
    val bImg = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB)
    val img = new RTImage {
      def width = bImg.getWidth()

      def height = bImg.getHeight()

      def setColor(x: Int, y: Int, color: RTColor): Unit = {
        bImg.setRGB(x, y, color.toARGB)
      }
    }
    val lights: List[PointLight] = List(
      PointLight(new RTColor(0.9, 0.9, 0.9, 1), Point(1e-1, 0, 1e-2)), PointLight(new RTColor(0.5, 0.4, 0.1, 1), Point(-1e-1, 0, 1e-2))
    )
    val geom = new KDTreeGeometry[BoundingSphere](
      (0 until numSims).flatMap(
        i =>
          GeometrySetup.readRingWithOffsetSpheres(
            EricasTestingFile.cartAndRadNumbers(i),
            i * 2e-5 - (numSims-1) * 1e-5,
            0.0
          )
      )
    )
    println("Starting")
    val start = System.nanoTime()
    RayTrace.render(view._1, view._2, view._3, view._4, img, geom, lights, 1)
    println((System.nanoTime() - start) / 1e9)

    val frame = new JFrame {
      override def paint(g: Graphics): Unit = {
        g.drawImage(bImg, 0, 0, null)
      }
    }
    frame.setSize(size, size)
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    frame.setVisible(true)
  }
}
