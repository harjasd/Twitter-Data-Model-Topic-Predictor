package edu.ucr.cs.cs167.hdhal005
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.Map

object TemporalAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167_proj")
      .config(conf)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    import spark.implicits._
    val startDate = args(0)
    val endDate  = args(1)

    try {
      val df = spark.read.json("tweets_topic.json")
      df.createOrReplaceTempView("tweets")
      val filteredTweets = spark.sql("SELECT * FROM tweets WHERE country_code IS NOT NULL AND created_at IS NOT NULL")
      filteredTweets.show()

      filteredTweets.createOrReplaceTempView("filtered_tweets")

      val result = spark.sql(s"""
      SELECT country_code, COUNT(*) AS tweet_count
      FROM (
      SELECT *,
      to_timestamp(created_at, 'EEE MMM dd HH:mm:ss Z yyyy') AS timestamp,
      to_date('$startDate', 'MM/dd/yyyy') AS start_date,
      to_date('$endDate', 'MM/dd/yyyy') AS end_date
      FROM filtered_tweets
      ) WHERE timestamp BETWEEN start_date AND end_date
      GROUP BY country_code
      HAVING tweet_count > 50
      """)



      result.coalesce(1)
        .write
        .option("header", "true") // Include header
        .csv("CountryTweetsCount10KClean")

    } finally {
      spark.stop()
    }

  }

}
