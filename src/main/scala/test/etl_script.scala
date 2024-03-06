package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object etl_script {
    def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .appName("etl process")  // You can set your application name here
        .master("local[*]")  // Set the master URL here, e.g., "local[*]" for local mode
        .getOrCreate()
      Logger.getLogger("org").setLevel(Level.ERROR)

      val employeesDF = spark.read.option("header", "true").csv("hdfs://localhost:9000/test/employees.csv")
      val departmentsDF = spark.read.option("header", "true").csv("hdfs://localhost:9000/test/departments.csv")
      employeesDF.printSchema()

      // Renaming the column
      val empDF = employeesDF.withColumnRenamed("manager_id", "mng_id")
      //empDF.show()

      // Joining both df
      val joinedDF =   empDF.join(departmentsDF, "DEPARTMENT_ID")
      //joinedDF.show()

      // Department wise salary
      val departmentWiseSalary = joinedDF.groupBy("DEPARTMENT_NAME")
        .agg(sum("SALARY").alias("total_salary"))
      departmentWiseSalary.show()

      // Salary greater than 5000
      val highSalaryDF = joinedDF.filter(joinedDF("SALARY") > 5000)
      //highSalaryDF.show()

      // Count the number of employees in highSalaryDF
      val count = highSalaryDF.count()
      // Print the count
      println(s"Number of employees with salary greater than 5000: $count")

      // Write departmentSalaryDF to HDFS
      departmentWiseSalary.write.mode("overwrite").csv("hdfs://localhost:9000/test/departmentWiseSalary")

      // Write highSalaryDF to HDFS
      highSalaryDF.write.mode("overwrite").csv("hdfs://localhost:9000/test/highSalary")

      println("ETL Job Completed.")

    }
}
