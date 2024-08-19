package edu.ucr.cs.cs167.hdhal005

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}



import org.apache.spark.sql.{SparkSession, functions => F}

object DataPreparation2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Twitter Data Analysis")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read the JSON file
    val inputPath = args(0)
    val tweetsDF = spark.read.json(inputPath)

    // Correct path for hashtags according to your JSON structure
    val hashtagsColumn = "hashtags"

    // Define the list of top 20 hashtags
    val topHashtags = Seq(
      "ALDUBxEBLoveis", "FurkanPalalı", "no309", "LalOn", "chien",
      "job", "Hiring", "sbhawks", "Top3Apps",
      "perdu", "trouvé", "CareerArc", "Job", "trumprussia",
      "trndnl", "Jobs", "ShowtimeLetsCelebr8", "hiring", "impeachtrumppence", "music"
    )

    // Add a new column 'topic' to the DataFrame
    val tweetsWithTopic = tweetsDF
      .withColumn("hashtags", F.explode_outer($"$hashtagsColumn"))
      .withColumn("topic", F.when($"hashtags".isin(topHashtags: _*), $"hashtags"))
      .drop("hashtags")
      .dropDuplicates("id")
      .filter($"topic".isNotNull)


    // Select and rename the necessary columns to match the schema
    val finalDF = tweetsWithTopic
      .select(
        $"id", $"text", $"created_at", $"country_code",
        $"topic", $"user_description",
        $"retweet_count", $"reply_count", $"quoted_status_id"
      )


    // Show the schema to verify
    finalDF.printSchema()

    // Write the output to a JSON file
    val outputDir = "tweets_topic.json"

    // Write the DataFrame directly to a single JSON file
    //    finalDF.coalesce(1).write.json(outputDir)
    finalDF.coalesce(1).write.mode("overwrite").json(outputDir)


    spark.stop()
  }

}
