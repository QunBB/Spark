import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes


class Read_HBase {
    def read(): Unit = {
        val conf = HBaseConfiguration.create()
        val sparkConf = new SparkConf().setMaster("local").setAppName("HBase")
        val sc = new SparkContext(sparkConf)
        // 设置HBase的配置
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3,spark")
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase")
        conf.set("hbase.master", "master:60000")
        // 设置查询的表明
        conf.set(TableInputFormat.INPUT_TABLE, "spark_test")
        val RDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[Result])
        val count = RDD.count()
        print("The Count of RDD:" + count)
        RDD.cache()
        // 遍历输出
        RDD.foreach({case(_, result) =>
        val key = Bytes.toString(result.getRow)
        val first_name = Bytes.toString(result.getValue("name".getBytes, "first_name".getBytes))
        val last_name = Bytes.toString(result.getValue("name".getBytes(), "last_name".getBytes()))
        printf("ID: %s, first_name: %s, last_name: %s", key, first_name, last_name)

        })
    }

    def write(): Unit ={

    }
}


object HBase{
    def main(args: Array[String]): Unit = {
        val hbase = new Read_HBase
        hbase.read()
    }
}