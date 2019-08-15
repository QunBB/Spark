package SparkMl

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class MLPipeline {
    /**
      * 如何建立逻辑回归模型的机器学习工作流
      */
    def run(): Unit ={
        val spark: SparkSession = SparkSession.builder().master("local").
                appName("LogitRegression").getOrCreate()
        val trainData: DataFrame = spark.createDataFrame(Seq(
            (0L, "a b spark", 1.0),
            (1L, "a b", 1.0),
            (2L, "a cb spark", 1.0),
            (3L, "a spark", 1.0),
            (4L, "a spark v", 1.0),
            (5L, "a v", 1.0)
        )).toDF("id", "text", "label")
        // 对text字段进行分词，输出到word字段
        val tokenizer: Tokenizer = new Tokenizer().setInputCol("text").setOutputCol("word")
        // 将分词映射到特征向量
        val hashingTF: HashingTF = new HashingTF().setNumFeatures(1000).setInputCol(
            tokenizer.getOutputCol).setOutputCol("features")
        val lr: LogisticRegression = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
        // 构建工作流
        val pipeline: Pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
        val model: PipelineModel = pipeline.fit(trainData)
        // 对模型进行预测
        val testData: DataFrame = spark.createDataFrame(Seq(
            (5L, "a b spark"),
            (7L, "a b")
        )).toDF("id", "text")
        val result: DataFrame = model.transform(testData)
        result.select("id", "text", "probability", "prediction").collect().
                // 这里probability的格式需要注意，不是scala自带的Vector
                foreach{case Row(id: Long, text: String, probability: org.apache.spark.ml.linalg.Vector, prediction: Double) =>
                    println(s"($id, $text) --> prob=$probability, prediction=$prediction")}
    }
}


object MLPipeline{
    def main(args: Array[String]): Unit = {
        val lr = new MLPipeline
        lr.run()
    }
}