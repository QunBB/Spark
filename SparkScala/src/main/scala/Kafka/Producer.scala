package Kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

class Producer {
    /**
      *
      * @param brokers：kafka生产者的brokers
      * @param topic：kafka生产者的topic
      * @param messagesPerSec：每秒生成多少条消息
      * @param wordsPerMessage：每条消息包含多少个单词
      */
    def produceWords(brokers: String, topic: String, messagesPerSec: String, wordsPerMessage: String): Unit ={
        val props = new Properties()
        // 指定kafka服务器地址，以逗号隔开
        props.put("bootstrap.servers", brokers)
        // 指定序列化使用的类
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
        // 指定topic的group
        props.put("group.id", "test")
        val producer = new KafkaProducer[String, String](props)
        while (true){
            (1 to messagesPerSec.toInt).foreach({ messageNum =>
                val str = (1 to wordsPerMessage.toInt).map(x =>
                Random.nextInt(10).toString)
                        .mkString(" ") // 将集合中的所有元素拼成字符串并以空格隔开
                print(str)
                println()
                // 将数据写入kafka，topic-key-value
                val message = new ProducerRecord[String, String](topic, null, str)
                producer.send(message)
            })
            Thread.sleep(1000)
        }
    }
}

object Producer{
    def main(args: Array[String]): Unit = {
        val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
        val producer = new Producer
        producer.produceWords(brokers, topic, messagesPerSec, wordsPerMessage)
    }
}
