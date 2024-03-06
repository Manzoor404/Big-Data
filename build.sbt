version := "1.0"

scalaVersion := "2.12.10" // Adjust according to your requirements

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.828",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.11.1",
  "mysql" % "mysql-connector-java" % "8.0.26",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.6.7"
)

lazy val root = (project in file("."))
  .settings(
    name := "ETL Practice"
  )
