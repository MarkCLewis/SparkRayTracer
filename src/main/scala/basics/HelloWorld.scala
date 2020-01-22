package basics

import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer

/**
 * This is here to remind you how to write Scala and to make it so that
 * the directories for src actually go into the git repository.
 */
object HelloWorld {
	def main(args: Array[String]): Unit = {
		println("Hello World!")
		val plot = Plot.simple(ScatterStyle(1 to 10, (1 to 10).map(x => x*x)), "Grade vs. Effort", "Effort Required", "Grade")
		SwingRenderer(plot, 1000, 1000, true)
	}
}
