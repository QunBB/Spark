name := "KernelDensity_2.11"

organization := "com.hong.KernelDensity"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.0",
  "com.github.fommil.netlib" % "all" % "1.1.2"
)