package Kafka

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


class KafkaStreaming {
    /**
      * 建立spark streaming对kafka数据进行实时消费
      * 无状态转换，只统计本阶段的数据，无关历史数据
      */
    def DStream(): Unit = {
        val conf = new SparkConf().setAppName("Kafka").setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.sparkContext.setLogLevel("ERROR")
        // 检查点，检查点有容错机制
        ssc.checkpoint("resources/data/kafka")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092,spark:9092",
            "group.id" -> "test",
            // 指定反序列化的类
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean))

        val topics = Array("topicA", "topicB")

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        // 消费kafka的数据
        val lines = stream.map(record => (record.key, record.value))
        val wordCount = lines.flatMap(lines => lines._2.split(" ")).
                map(lines => (lines, 1)).reduceByKey(_ + _)
        wordCount.print()
        ssc.start()
        ssc.awaitTermination()
    }

    /**
      * 建立spark streaming对kafka数据进行实时消费
      * 有状态转换，对历史数据进行累计
      */
    def DStreamState(): Unit ={
        /**
          * 状态更新函数
          * 用于updateStateByKey函数，对历史数据进行累计，然后更新到当前状态
          * values：当前阶段对应key的value集合
          * state：历史状态对应key是否存在
          */
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentValue = values.foldLeft(0)(_ + _) // 当前阶段的value-reduce操作
            val previousValue = state.getOrElse(0) // 如果历史存在则返回原value，不存在则赋值0
            Some(currentValue + previousValue) // 历史和现阶段累加并返回
        }

        val conf = new SparkConf().setAppName("Kafka").setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.sparkContext.setLogLevel("ERROR")
        // 检查点，检查点有容错机制
        ssc.checkpoint("resources/data/kafka")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092,spark:9092",
            "group.id" -> "test",
            // 指定反序列化的类
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean))

        val topics = Array("topicA", "topicB")

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        // 消费kafka的数据
        val lines = stream.map(record => (record.key, record.value))
        val wordCount = lines.flatMap(lines => lines._2.split(" ")).
                map(lines => (lines, 1))
        val stateCount = wordCount.updateStateByKey[Int](updateFunc)
        stateCount.print()
        ssc.start()
        ssc.awaitTermination()
    }

    /**
      * 建立spark streaming对kafka数据进行实时消费并写入mysql数据库
      */
    def streamingWriteMysql(): Unit ={
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentValue = values.foldLeft(0)(_ + _) // 当前阶段的value-reduce操作
            val previousValue = state.getOrElse(0) // 如果历史存在则返回原value，不存在则赋值0
            Some(currentValue + previousValue) // 历史和现阶段累加并返回
        }

        val conf = new SparkConf().setAppName("Kafka").setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.sparkContext.setLogLevel("ERROR")
        // 检查点，检查点有容错机制
        ssc.checkpoint("resources/data/kafka")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092,spark:9092",
            "group.id" -> "test",
            // 指定反序列化的类
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean))

        val topics = Array("topicA", "topicB")

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        // 消费kafka的数据
        val lines = stream.map(record => (record.key, record.value))
        val wordCount = lines.flatMap(lines => lines._2.split(" ")).
                map(lines => (lines, 1))
        val stateCount = wordCount.updateStateByKey[Int](updateFunc)
        stateCount.print()
        stateCount.foreachRDD(rdd =>{
            // 定义内部函数：将RDD的数据写入mysql
            def write(records: Iterator[(String, Int)]): Unit ={
                var conn: Connection = null
                var stmt: PreparedStatement = null
                try{
                    // 建立数据库链接
                    val url = "jdbc:mysql://localhost:3306/table_name"
                    val username = "root"
                    val password = "hong123"
                    conn = DriverManager.getConnection(url, username, password)
                    records.foreach(p => {
                        val sql = "insert into table(t1, t2) values (?, ?)"
                        stmt = conn.prepareStatement(sql)
                        stmt.setString(1, p._1.trim)
                        stmt.setInt(2, p._2.toInt)
                        stmt.executeUpdate()
                    })
                } catch {
                    case e: Exception => e.printStackTrace()
                } finally {
                    if (stmt != null) stmt.close()
                    if (conn != null) conn.close()
                }
            }
            val repartionedRDD = rdd.repartition(3)
            repartionedRDD.foreachPartition(write)
        })
        ssc.start()
        ssc.awaitTermination()
    }
}


object  Run{
    def main(args: Array[String]): Unit = {
        val ks = new KafkaStreaming
        ks.DStreamState()
    }

}