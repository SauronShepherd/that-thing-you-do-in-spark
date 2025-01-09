organization := "spark-wtf"
name := "that-thing-you-do-in-spark"
version := "1.0-SNAPSHOT"

scalaVersion := "2.12.17"

lazy val sparkVersion = "3.5.4"
lazy val postgresqlVersion = "42.7.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % postgresqlVersion

)