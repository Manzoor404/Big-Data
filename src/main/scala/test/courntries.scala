package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, _}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => F}
import scala.io.Source
import org.apache.spark.sql.Column
import scala.language.postfixOps
import scala.reflect.internal.util.NoPosition.show // Need, to read data from URL
object courntries {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val Conf = new SparkConf()
      .setAppName("SchemaRDD") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,


    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()

    //Reading data from URL as String ,using common template(line:24,25)
    val htmlSource = Source.fromURL("https://data.covid19india.org/v4/min/data.min.json")
    val fileAsString = htmlSource.mkString
    // println(fileAsString)

    // Converting URL data(string) to RDD
    val toRDD = sc.parallelize(List(fileAsString))
      //toRDD.foreach(println)


    // Reading Above RDD as DataFrame
    val df = spark.read.json(toRDD)
    val districtsData = df.select(F.col("AN.districts.Nicobars.delta7.vaccinated1"))
    districtsData.show(false)
    //println("=====flattened schema=====")
    //df.printSchema()
    //df.show()

    //flattening data into state-wise
    /*val allStatesData = df.select(
        col("AN")
      , col("AP")
      , col("AR")
      , col("AS")
      , col("BR")
      , col("CH")
      , col("CT")
      , col("DL")
      , col("DN")
      , col("GA")
      , col("GJ")
      , col("HP")
      , col("HR")
      , col("JH")
      , col("JK")
      , col("KA")
      , col("KL")
      , col("LA")
      , col("LD")
      , col("MH")
      , col("ML")
      , col("MN")
      , col("MP")
      , col("MZ")
      , col("NL")
      , col("OR")
      , col("PB")
      , col("PY")
      , col("RJ")
      , col("SK")
      , col("TG")
      , col("TN")
      , col("TR")
      , col("TT")
      , col("UP")
      , col("UT")
      , col("WB")
    )
    allStatesData.show(false)
    //allStatesData.printSchema()*/

    val stateCodes = List("AN", "AP","AR", "AS", "BR","CH", "CT", "DL", "DN", "GA", "GJ","HP", "HR", "JH", "JK", "KA", "KL", "LA", "LD","MH", "ML", "MN", "MP", "MZ", "NL", "OR", "PB", "PY", "RJ", "SK", "TG", "TN", "TR", "UP", "UT", "WB")
    val stateNames = Map(
      "AN" -> "Andaman and Nicobar Islands",
      "AP" -> "Andhra Pradesh",
      "AR" -> "Arunachal Pradesh",
      "AS" -> "Assam",
      "BR" -> "Bihar",
      "CH" -> "Chandigarh",
      "CT" -> "Chhattisgarh",
      "DL" -> "Delhi",
      "DN" -> "Dadra and Nagar Haveli and Daman and Diu",
      "GA" -> "Goa",
      "GJ" -> "Gujarat",
      "HP" -> "Himachal Pradesh",
      "HR" -> "Haryana",
      "JH" -> "Jharkhand",
      "JK" -> "Jammu and Kashmir",
      "KA" -> "Karnataka",
      "KL" -> "Kerala",
      "LA" -> "Ladakh",
      "LD" -> "Lakshadweep",
      "MH" -> "Maharashtra",
      "ML" -> "Meghalaya",
      "MN" -> "Manipur",
      "MP" -> "Madhya Pradesh",
      "MZ" -> "Mizoram",
      "NL" -> "Nagaland",
      "OR" -> "Odisha",
      "PB" -> "Punjab",
      "PY" -> "Puducherry",
      "RJ" -> "Rajasthan",
      "SK" -> "Sikkim",
      "TG" -> "Telangana",
      "TN" -> "Tamil Nadu",
      "TR" -> "Tripura",
      "UP" -> "Uttar Pradesh",
      "UT" -> "Uttarakhand",
      "WB" -> "West Bengal"
    )
    val fullStateNames = stateCodes.map(code => s"${stateNames(code)}.districts")
    //val districtsColumns = stateCodes.map(code => col(s"$code.districts"))
    //val allDistrictsData = df.select(districtsColumns: _*)
    //allDistrictsData.printSchema()
    //allDistrictsData.show(false)




    //write the json to parquet
    //df.write.mode("overwrite").parquet("hdfs://localhost:9000/test/countries.parquet")

    println("---ETL Job Completed---")


  }
}
