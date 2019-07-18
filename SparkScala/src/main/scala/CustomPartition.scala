import org.apache.spark.{SparkContext, SparkConf, Partitioner}


class CustomPartition {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(conf)

        val data = sc.parallelize(1 to 10, 5) // 5个分区数
        // val data = sc.textFile("", 5)

        // 不同的分区，写入到不同得对应文件
        data.map((_, 1)).partitionBy(new CustomPartitioner(10)).map(_._1).saveAsTextFile("")
    }
}


class CustomPartitioner(num: Int) extends Partitioner{
    // 重载分区数
    override def numPartitions: Int = num
    // 重载根据key获取分区号的方法
    override def getPartition(key: Any): Int = {
        key.toString.toInt % 10
    }
}
