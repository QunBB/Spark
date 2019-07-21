import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job


/**
  * Read from HBase and Write to HBase in Spark
  */
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
        // 注意TableInputFormat需要从org.apache.hadoop.hbase.mapreduce模块导入
        val RDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[Result])
        val count = RDD.count()
        print("The Count of RDD:" + count)
        RDD.cache()
        // 遍历输出
        RDD.foreach({case(_, result) =>
        val key = Bytes.toString(result.getRow)
        // name是列族，first_name是列名
        val first_name = Bytes.toString(result.getValue("name".getBytes, "first_name".getBytes))
        val last_name = Bytes.toString(result.getValue("name".getBytes(), "last_name".getBytes()))
        printf("ID: %s, first_name: %s, last_name: %s", key, first_name, last_name)

        })
    }

    def write(): Unit ={
        val conf = HBaseConfiguration.create()
        val sparkConf = new SparkConf().setMaster("local").setAppName("HBase")
        val sc = new SparkContext(sparkConf)
        // 设置HBase的配置
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3,spark")
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase")
        conf.set("hbase.master", "master:60000")
        conf.set(org.apache.hadoop.hbase.mapred.TableOutputFormat.OUTPUT_TABLE, "spark_test")
        val job = new JobConf(conf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
        // 输出表的设置可在job或conf二选一
//        job.set(TableOutputFormat.OUTPUT_TABLE, "spark_test")
        val data = sc.makeRDD(Array("2,qun,zhang", "3,yingjun,zhang"))
        val rdd = data.map(_.split(",")).map{arr => {
            val put = new Put(Bytes.toBytes(arr(0))) // 行id
            put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first_name"), Bytes.toBytes(arr(1)))
            put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first_name"), Bytes.toBytes(arr(2)))
            (new ImmutableBytesWritable, put)
        }}
        rdd.saveAsHadoopDataset(job)
    }

    def writeNew(): Unit ={
        val sparkConf = new SparkConf().setMaster("local").setAppName("HBase")
        val sc = new SparkContext(sparkConf)
        // 设置HBase的配置
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "slave1,slave2,slave3,spark")
        sc.hadoopConfiguration.set("hbase.rootdir", "hdfs://master:9000/hbase")
        sc.hadoopConfiguration.set("hbase.master", "master:60000")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "spark_test")

        val job = Job.getInstance(sc.hadoopConfiguration)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        // org.apache.hadoop.hbase.mapreduce.TableOutputFormat
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        val data = sc.makeRDD(Array("2,qun,zhang", "3,yingjun,zhang"))
        val rdd = data.map(_.split(",")).map{arr => {
            val put = new Put(Bytes.toBytes(arr(0))) // 行id
            put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first_name"), Bytes.toBytes(arr(1)))
            put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first_name"), Bytes.toBytes(arr(2)))
            (new ImmutableBytesWritable, put)
        }}
        rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
}


object HBase{
    def main(args: Array[String]): Unit = {
        val hbase = new Read_HBase
        hbase.write()
    }
}