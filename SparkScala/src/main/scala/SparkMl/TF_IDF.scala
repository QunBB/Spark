package SparkMl

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TF_IDF {
    /**
      * 首先通过HashingTF进行特征向量映射
      * 然后训练IDF度量模型
      */
    def model(): Unit ={
        val spark: SparkSession = SparkSession.builder().master("local").
                appName("LogitRegression").getOrCreate()
        val sentences: DataFrame = spark.createDataFrame(Seq(
            (0L, "i heard about your daddy"),
            (1L, "i love you and your brother"),
            (2L, "spark is a good tool")
        )).toDF("id", "text")
        // 对text字段进行分词，输出到word字段
        val tokenizer: Tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
        val words = tokenizer.transform(sentences)
        words.show(false) // 全部不省略的展示字段的所有内容
        // 将分词映射到特征向量
        val hashingTF: HashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").
                setOutputCol("features")
        val vectors = hashingTF.transform(words)
        vectors.show(false)
        // 构建IDF Estimator
        val idf = new IDF().setInputCol("features").setOutputCol("IDF")
        val idfModel = idf.fit(vectors)
        val result = idfModel.transform(vectors)
        result.show(false)

    }
}


object Run{
    def main(args: Array[String]): Unit = {
        val model = new TF_IDF
        model.model()
    }
}
