package edu.ucr.cs.cs167.hdhal005

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, udf}

import scala.collection.mutable

object DataPreparation1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: PreprocessTweets <input_file_path>")
      System.exit(1)
    }
    val inputfile: String = args(0)

    val getHashtagTexts: UserDefinedFunction = udf((x: mutable.WrappedArray[GenericRowWithSchema]) => {
      x match {
        case x: mutable.WrappedArray[GenericRowWithSchema] => x.map(_.getAs[String]("text"))
        case _ => null
      }
    })
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Project - Data Preparation")
      .config(conf)
      .getOrCreate()
    spark.udf.register("getHashtagTexts", getHashtagTexts)

    try {
      import spark.implicits._
      var df = spark.read.json(inputfile)
      df.printSchema()

      df.createOrReplaceTempView("tweets")
      df = df.select(
        col("id").cast("long"),
        col("text").cast("string"),
        col("created_at").cast("string"),
        col("place.country_code").alias("country_code"),
        col("entities.hashtags.text").alias("hashtags"),
        col("user.description").alias("user_description"),
        col("retweet_count").cast("long"),
        col("reply_count").cast("long"),
        col("quoted_status_id").cast("long")
      )

      val outputfile = "tweets_clean10k.json"
      df.write.json(outputfile)
      df.printSchema()
      df.show()


      df = df.withColumn("hashtag", explode($"hashtags"))
      df.createOrReplaceTempView("tweets")
      df = spark.sql(s"SELECT hashtag, COUNT(*) as count FROM tweets GROUP BY hashtag ORDER BY count DESC LIMIT 20")

      df.printSchema()
      df.show(20)

      val hashtagsArray = df.select("hashtag").as[String].collect()
      println("\nTop 20 hashtags: ")
      hashtagsArray.foreach(println)


    } finally {
      spark.stop
    }
  }
}



