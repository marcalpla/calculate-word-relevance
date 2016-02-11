name := "CalculateWordRelevance"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  "net.sf.opencsv" % "opencsv" % "2.0"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
