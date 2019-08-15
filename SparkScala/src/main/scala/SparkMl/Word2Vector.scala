package SparkMl

import org.apache.spark.ml.feature.{CountVectorizer, Word2Vec}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Word2Vector {
    /**
      * 通过Word2Vector将文本进行特征向量映射
      */
    def word2VecModel(): Unit ={
        val spark: SparkSession = SparkSession.builder().master("local").
                appName("LogitRegression").getOrCreate()

        val sentences: DataFrame = spark.createDataFrame(Array(
            "i heard about your daddy".split(" "),
            "i love you and your brother".split(" "),
            "spark is a good tool".split(" ")
        ).map(Tuple1.apply)).toDF("text")
        // MinCount：最少出现多少次才进行特征向量映射
        val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("vector").
                setMinCount(0).setMaxIter(10)
        val model = word2Vec.fit(sentences)
        val result = model.transform(sentences)
        result.show(false)
    }

    def countVecModel(): Unit ={
        /**
          * 通过Word2Vector将文本进行特征向量映射
          */
        val spark: SparkSession = SparkSession.builder().master("local").
                appName("LogitRegression").getOrCreate()

        val sentences: DataFrame = spark.createDataFrame(Array(
            "i heard about your daddy".split(" "),
            "i love you and your brother".split(" "),
            "spark is a good tool".split(" ")
        ).map(Tuple1.apply)).toDF("text")
        val countVec = new CountVectorizer().setInputCol("text").setOutputCol("vector").
                setVocabSize(3).setMinDF(2).fit(sentences)
        countVec.vocabulary.foreach(println)
        countVec.transform(sentences).show(false)
    }
}
