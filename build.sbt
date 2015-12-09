name := "Scala_3"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"

/*
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.8.0"
*/

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)