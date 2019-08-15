package SparkMl

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

case class Iris(features: org.apache.spark.ml.linalg.Vector, label: String)

class LogitRegression {
    /**
      * 逻辑斯蒂回归模型
      */
    def model(): Unit ={
        val spark: SparkSession = SparkSession.builder().master("local").
                appName("LogitRegression").getOrCreate()
        import spark.implicits._
        // 将Array转化为Vectors
        val data = spark.sparkContext.textFile("data.txt").map(_.split(",")).
                map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble), p(3))).
                toDF()
        data.createOrReplaceTempView("iris")
        val df = spark.sql("select * from iris where label != 'c'")
        // 构建样本特征和label的index
        val labelIndex = new StringIndexer().setInputCol("label").setOutputCol("label_id").
                fit(df)
        val featureIndex = new VectorIndexer().setInputCol("features").setOutputCol("feature_id").
                fit(df)
        // 随机划分训练集和测试机
        val Array(train, test) = df.randomSplit(Array(0.7, 0.3))
        val lr = new LogisticRegression().setLabelCol("label_id").setFeaturesCol("feature_id").
                setRegParam(0.3).setMaxIter(100).setElasticNetParam(0.8)
        // 将label的index转化为原label的string
        val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("pre_label").
                setLabels(labelIndex.labels)
        // 构建工作流
        val lrPipeline = new Pipeline().setStages(Array(
            labelIndex, featureIndex, lr, labelConverter))
        val model = lrPipeline.fit(train)
        val result = model.transform(test)
        // 构建模型的评估器
        val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label_id").
                setPredictionCol("prediction")
        val acc = evaluator.evaluate(result)
        // 获取模型的相关参数
        val lrModel = model.stages(2).asInstanceOf[LogisticRegressionModel]
        println(lrModel.coefficients)
    }
}
