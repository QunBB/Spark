import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.parsing.json.JSON


object RDD {
    def main(args: Array[String]): Unit ={
        val conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(conf)

        // 读取文件的方式创建RDD
        // Linux默认是读取hdfs
        val lines = sc.textFile("hdfs://localhost:9000/scala/word.txt")

        // 从集合创建RDD
        val arr = Array(1, 2, 3, 4)
        val rdd = sc.parallelize(arr)

        // 转换操作: filter, map, flatMap
        val transform = lines.flatMap(line => line.split(" ")).map((_, 1))
        // 键值对RDD转换操作：groupByKey, reduceByKey, keys, values, sortByKey, sortBy
        val g = transform.groupByKey()
        // (a, 1),(b, 1),(a, 2),(b, 2) => (a, (1, 2)), (b, (1, 2))

        // 行动操作: count, reduce, foreach, take, first
        val action = transform.count()

        // RDD持久化，重复使用，不需从头开始计算。
        // persist(MEMORY_ONLY)：对RDD反序列化存储于JVM，留在内存 = cache()
        // persist(MEMORY_AND_DISK)：内存不足时，会存在硬盘
        rdd.cache()
        rdd.unpersist() // 从缓存中删除

        // 设置分区
        rdd.repartition(2)
        rdd.partitions.size // 查看分区数

        // 共享变量
        // 广播变量，只读不能修改，在每台机器缓存
        val broadcastVar = sc.broadcast(Array(1, 2, 3)) // 可以传递任何类型的变量
        broadcastVar.value
        // 累加器，任务只能累加，不能读取变量
        val accumulator = sc.longAccumulator("Accumulator")
        rdd.foreach(x => accumulator.add(x))
        accumulator.value // 只能在Driver Program可以读取值

        // 文件写入
        lines.saveAsTextFile("result") // 保存至result目录下，与hdfs一样
        val in = sc.textFile("result") // 无需具体文件，将目录下的所有分块文件全部读取

        // 解析JSON字符串
        val json = lines.map(x => JSON.parseFull(x))
        json.foreach({r => r match {
            case Some(map: Map[String, Any]) => println(map)
            case None => println("Failed") // 解析失败则返回None
        }})


    }
}
