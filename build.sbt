name := "BigData and Machine Learning"
 
version := "1.0"

scalaVersion := "2.12.10"

fork := true

/*libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.192-R14"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"
*/
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3" % "provided"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
