import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._


class SparkSql {
    def json2df(): Unit ={
        /**
          * 读取json文件数据，转化为dataframe
          */
        val sparkConf = new SparkConf().setMaster("local").setAppName("SparkSql")
        val sc = new SparkContext(sparkConf)
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._ // 这里的spark是上面的spark变量
        // 与spark.read.format("json").load相同
        val df = spark.read.json("resources/data/SparkSql/test1.json")
        df.show() // 打印表数据
        df.printSchema() // 打印表结构
        // 选择特定的列以及重命名
        df.select(df("name"), df("name") as("rename"), df("age")+1).show()
        df.filter(df("age") > 1).show() // 条件过滤
        df.groupBy("age").count().show() // 分组统计
        df.sort(df("age").desc, df("name")).show() // 排序
    }

    def rdd2df(): Unit ={
        /**
          * 通过反射机制推断，将RDD转化成dataframe
          */
        val sparkConf = new SparkConf().setMaster("local").setAppName("SparkSql")
        val sc = new SparkContext(sparkConf)
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        // Person类必须在外面定义
        val df = spark.sparkContext.textFile("resources/data/SparkSql/rdd2df.txt").map(_.split(",")).
                map(x => Person(x(0), x(1).trim.toInt)).toDF()
        df.createOrReplaceTempView("people") // 必须注册为临时表才能提供sql查询
        val rdd = spark.sql("select name, age from people") // 返回dataframe
        rdd.show()
        rdd.map(x => "Name:" + x(0) + ", Age:" + x(1)).show() // 格式化打印
    }

    def rdd2df2(): Unit ={
        /**
          * 通过编程方式定义RDD模式，将RDD转化成dataframe
          */
        val sparkConf = new SparkConf().setMaster("local").setAppName("SparkSql")
        val sc = new SparkContext(sparkConf)
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val personRDD = spark.sparkContext.textFile("resources/data/SparkSql/rdd2df.txt")
        val schemaString = "name,age"
        // 对RDD模式中的每个字段进行定义
        val fields = schemaString.split(",").map(fieldName =>
            StructField(fieldName, StringType, nullable = true))
        // RDD模式
        val schema = StructType(fields)
        val rowRDD = personRDD.map(_.split(",")).map(arrt => Row(arrt(0), arrt(1)))
        // 将Row与模式结合起来
        val df = spark.createDataFrame(rowRDD, schema)
        df.createOrReplaceTempView("people") // 必须注册为临时表才能提供sql查询
        val rdd = spark.sql("select name, age from people") // 返回dataframe
        rdd.show()
        rdd.map(x => "Name:" + x(0) + ", Age:" + x(1)).show() // 格式化打印
    }

    def RDDSave(): Unit ={
        /**
          * 保存dataframe至文件系统
          */
        val sparkConf = new SparkConf().setMaster("local").setAppName("SparkSql")
        val sc = new SparkContext(sparkConf)
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._ // 这里的spark是上面的spark变量
        // 与spark.read.format("json").load相同
        val df = spark.read.json("resources/data/SparkSql/test1.json")
        df.show() // 打印表数据
        df.printSchema() // 打印表结构
        df.select("name", "name").write.format("csv").save("writeDF.csv")
        df.rdd.saveAsTextFile("writeRDD.txt")
    }

    def connJDBC(): Unit ={
        /**
          * 通过jdbc连接mysql，进行数据读写操作
          */
        val sparkConf = new SparkConf().setMaster("local").setAppName("SparkSql")
        val sc = new SparkContext(sparkConf)
        val spark = SparkSession.builder().getOrCreate()
        // 读取数据库的数据
        // 设置数据库连接信息
        val options = Map("user" -> "root", "password" -> "woaini000", "url" -> "jdbc:mysql://localhost:3306/sys",
            "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "sys_config")
        val df = spark.read.format("jdbc").options(options).load()
        df.show()
        // 将dataframe写入数据库
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "")
        prop.put("driver", "com.mysql.jdbc.Driver")
        // sys数据库名称
        df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/sys",
            "sys.sys_config", prop)

    }

}

case class Person(name: String, age: Int)

object SparkSql{
    def main(args: Array[String]): Unit = {
        val sparkSql = new SparkSql
        sparkSql.connJDBC()
    }
}
