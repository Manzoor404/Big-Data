version := "1.0"
scalaVersion := "2.12.10" // Adjust according to your requirements
libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.2.24", // JDBC Connector Dependency
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.5",
  "com.amazonaws" % "amazon-kinesis-client" % "1.13.3",
  "com.amazonaws" % "aws-java-sdk-core" % "1.11.820",
  "com.amazonaws" % "aws-java-sdk" % "1.11.811",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.828",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.886",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.797",
  "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.11.821",
  "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.744",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.11.1",
  "mysql" % "mysql-connector-java" % "8.0.26",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.6.7"
)

lazy val root = (project in file("."))
  .settings(
    name := "ETL Practice"
  )