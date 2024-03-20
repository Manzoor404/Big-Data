package test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Zomato_Restaurants_India {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Zomato_Restaurants_India")  // You can set your application name here
      .master("local[*]")  // Set the master URL here, e.g., "local[*]" for local mode
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

  }
}
