package SparkStreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class SparkStreaming {
    def fileStreaming(): Unit ={
        /**
          * 监听文件夹的新增文件内容
          */
        // 至少要启动2个线程以上，1个用于监听，1个用于处理数据
        val conf = new SparkConf().setMaster("local[8]").setAppName("SparkSql")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(3))
        // 只会读取在监听期间传入监听文件夹的文件
        // 并且该文件还必须在开始监听之后进行修改过
        val lines = ssc.textFileStream("resources/data/SparkStreaming.SparkStreaming/wordCount.txt")
        val words = lines.flatMap(_.split(","))
        val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCount.print()
        ssc.start()
        ssc.awaitTermination()
    }

    def socketStreaming(): Unit ={
        /**
          * 监听socket端口的写入内容
          */
        val conf = new SparkConf().setMaster("local[4]").setAppName("SparkSql")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(3))
        val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
        val words = lines.flatMap(_.split(","))
        val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCount.print()
        ssc.start()
        ssc.awaitTermination()
    }

    def RDDStreaming(): Unit ={
        /**
          * 创建RDD流模拟SparkStreaming
          */
        val conf = new SparkConf().setMaster("local[4]").setAppName("SparkSql")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(3))
        val RDDQueue = new mutable.Queue[RDD[Int]]
        val queueStream = ssc.queueStream(RDDQueue)
        val counts = queueStream.map(r => (r % 10, 1)).reduceByKey(_ + _)
        counts.print()
        ssc.start()

        for (i <- 1 to 10){
            RDDQueue += ssc.sparkContext.makeRDD(1 to 100, 2)
            Thread.sleep(1000)
        }
        ssc.stop()
    }

    def windowedDStream(): Unit ={
        /**
          * 监听socket端口，对Window Operations进行测试
          */
        val conf = new SparkConf().setMaster("local[4]").setAppName("SparkSql")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(1))
        val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
        // window：3，slide：2
        // 每隔2秒钟对最近3秒的数据进行一次reduce操作
        val wordCount = lines.map(x => x.toLong).reduceByWindow(_ + _, Seconds(3), Seconds(2))
        wordCount.print()
        ssc.start()
        ssc.awaitTermination()
    }
}


object Test{
    def main(args: Array[String]): Unit = {
        val sparkStreaming = new SparkStreaming
        sparkStreaming.windowedDStream()
    }
}