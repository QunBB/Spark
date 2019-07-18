import org.apache.spark.{SparkConf, SparkContext}


class SparkYarn extends Serializable {
    def yarnConnection(): Unit ={
        val conf = new SparkConf().setAppName("Spark Yarn").setMaster("yarn").set("deploy-mode", "client")
        System.setProperty("hadoop.home.dir","D:\\Hadoop\\hadoop")
        System.setProperty("HADOOP_USER_NAME", "hdfs")
//        System.setProperty("HADOOP_CONF_DIR", "D:\\Hadoop\\hadoop\\etc\\hadoop")

        conf.set("spark.yarn.jar", "hdfs://master:9000/SparkJars/*.jar")
        conf.set("yarn.resourcemanager.hostname", "master")
//        conf.set("spark.drive.host", "master")
        conf.set("spark.yarn.dist.files", "D:\\Hadoop\\Scala\\SparkRDD\\resources\\yarn-site.xml")
        conf.set("spark.yarn.preserve.staging.files","false")
//        conf.setJars(List("D:\\Hadoop\\Scala\\SparkRDD\\out\\artifacts\\SparkRDD_jar"))

        val sc = new SparkContext(conf)

        val lines = sc.textFile("/test.txt").flatMap(line => line.split(" ")).map(word => (word, 1))
        val wordNum = lines.reduceByKey(_ + _)

//        wordNum.foreach(println)
        wordNum.saveAsTextFile("result")


    }
}

object Run{
    def main(args: Array[String]): Unit = {
        val sy = new SparkYarn()
        sy.yarnConnection()
    }
}
