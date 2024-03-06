package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source // Need, to read data from URL
object WebAPI_ETL {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val Conf = new SparkConf()
      .setAppName("SchemaRDD") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()

    //Reading data from URL as String ,using common template(line:24,25)
    val htmlSource = Source.fromURL("https://randomuser.me/api/0.8/?results=20")
    val fileAsString = htmlSource.mkString
    // println(fileAsString)

    // Converting URL data(string) to RDD
    val toRDD = sc.parallelize(List(fileAsString))
    //  toRDD.foreach(println)


    // Reading Above RDD as DataFrame
    val df = spark.read.json(toRDD)
    println()
    println("=====flattened schema=====")
    df.printSchema()
    df.show()

    //======Tranformation, flattening the data======

    val flatarray = df.withColumn("results", explode(col("results")))
    flatarray.printSchema()

    val flatstruct = flatarray.select(
      "nationality",
      "results.user.cell",
      "results.user.dob",
      "results.user.email",
      "results.user.gender",
      "results.user.md5",
      "results.user.password",
      "results.user.phone",
      "results.user.registered",
      "results.user.salt",
      "results.user.sha1",
      "results.user.sha256",
      "results.user.username",
      "results.user.location.*",
      "results.user.name.*",
      "results.user.picture.*",
      "seed",
      "version"
    )
    println()
    println("=====flattened schema=====")
    flatstruct.printSchema()
    flatstruct.show(20, false)


    // Writing the data to hdfs as parquet
     flatstruct.write.mode("overwrite").parquet("hdfs://localhost:9000/test/WebAPI.parquet")
    println("---ETL Job Completed---")


  }
}
