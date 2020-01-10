name := "SparkScala"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.4.3",
    "org.apache.spark" % "spark-yarn_2.11" % "2.4.3",
    "org.apache.spark" % "spark-streaming_2.11" % "2.4.3",
    "org.apache.spark" % "spark-mllib_2.11" % "2.4.3",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3",
    "org.apache.spark" % "spark-sql_2.11" % "2.4.3",
    "org.apache.hbase" % "hbase-client" % "2.1.5",
    "org.apache.hbase" % "hbase-common" % "2.1.5",
    "org.apache.hbase" % "hbase-server" % "2.1.5",
    "org.apache.hbase" % "hbase-mapreduce" % "2.1.5",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.2",
    "com.alibaba" % "fastjson" % "1.2.47",
    "com.google.code.gson" % "gson" % "2.8.5",
    "redis.clients" % "jedis" % "2.9.0"
)