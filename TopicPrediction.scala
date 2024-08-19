package edu.ucr.cs.cs167.gyaye001

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassifier

object TopicPrediction {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setAppName("Tweet Topic Prediction")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()


    val inputFile: String = args(0)

    val classifiedTweetsDF = sparkSession.read
      .option("header", "true")
      .json(inputFile)

    classifiedTweetsDF.show()
    
       println("Data Indicator System ")

    
    println(" Sample Data ")
    classifiedTweetsDF.show(5, truncate = false)

    // Check for missing values in the dataset
    println("=== Missing Values ===")
    classifiedTweetsDF.select(
  classifiedTweetsDF.columns.map(c => sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).alias(c)): _*
).show()

    // Show summary statistics for numeric columns
    println("=== Data Summary Statistics ===")
    classifiedTweetsDF.describe().show()

    // Show class distribution of the target variable (topic)
    println("=== Topic Distribution ===")
    classifiedTweetsDF.groupBy("topic").count().show()
    
    
     println(" End of Data Indicator System ")

    val Array(trainingData, testData) = classifiedTweetsDF.randomSplit(Array(0.8, 0.2), seed = 1234)

    // Load the sample dataset in JSON format

    // Tokenize the text and user description
    val tokenizer = new Tokenizer()
      .setInputCol("text") // Use setInputCol instead of setInputCols
      .setOutputCol("tokens")

    // Convert tokens to numeric features
    val hashingTF = new HashingTF()
      .setInputCol("tokens")
      .setOutputCol("features")

    // Convert topics to numeric indices
    val topicIndexer = new StringIndexer()
      .setInputCol("topic")
      .setOutputCol("label")
      .fit(classifiedTweetsDF)


    // Train a Logistic Regression model

    val logisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, topicIndexer, logisticRegression))


    /*
    val randomForest = new RandomForestClassifier()
      .setNumTrees(10) // Number of trees in the forest
      .setMaxDepth(5) // Maximum depth of each tree



    // Create a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, topicIndexer, randomForest))

 */


    // Fit the pipeline to the training data
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)


    //val model = pipeline.fit(classifiedTweetsDF)
    //val allTweetsDF = sparkSession.read.json(inputFile)
    //val predictions = model.transform(allTweetsDF)


    predictions.select("id", "text", "topic", "user_description", "label", "prediction").show(false)

    val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    // Calculate overall  precision
    val precision = precisionEvaluator.evaluate(predictions)

    // Initialize the evaluator for  recall
    val recallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    // Calculate overall  recall
    val recall = recallEvaluator.evaluate(predictions)

    println(s"Overall  Precision: $precision")
    println(s"Overall  Recall: $recall")
  }
}
