version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "WordCountScala2"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2"
)
