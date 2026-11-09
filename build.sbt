ThisBuild / scalaVersion := "2.13.18"

name := "HW2-MongoDB-SparkSQL"

version := "0.1.0"

libraryDependencies ++= Seq(
  "org.mongodb" % "mongodb-driver-sync" % "5.2.1",
  "org.apache.spark" %% "spark-sql" % "4.2.0" % "provided"
)