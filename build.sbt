name := "Scala_3"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "compile",
  "org.apache.spark" %% "spark-sql" % "1.5.2" % "compile",
  "javax.servlet" % "javax.servlet-api" % "3.0.1" % "compile"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)