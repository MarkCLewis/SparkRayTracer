name := "SparkRayTracer"
 
version := "1.0"

scalaVersion := "2.12.10"

//javaOptions += "-Xmx12G"


fork := true

libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.192-R14"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.1"

 //libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1" % "provided"
 //libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided"
 //libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1" % "provided"
 //libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.1" % "provided"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
