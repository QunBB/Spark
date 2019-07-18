name := "SparkScala"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-yarn_2.11" % "2.4.3"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.1.5"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.1.5"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.1.5"
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.1.5"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.2"