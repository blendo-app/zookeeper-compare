import sbt.Keys._
import sbt._

enablePlugins(PackPlugin)


name := "zookeeper-compare"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies := Seq(
  "org.apache.curator" % "curator-recipes" % "2.12.0",
  "com.github.scopt" %% "scopt" % "3.7.0"
)
