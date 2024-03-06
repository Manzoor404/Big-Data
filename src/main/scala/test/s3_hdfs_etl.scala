package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object s3_hdfs_etl {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("AWS_S3_Spark_HDFS")
      .config("spark.hadoop.fs.s3a.access.key", "AKIAQYMN6MKPTKHV5F4E")
      .config("spark.hadoop.fs.s3a.secret.key", "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")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Read data from S3
    val df = spark.read.format("csv").option("header", "true").load("s3a://syedmanzoor/data/employees.csv")
    df.show()

  }
}
