version := "1.0"

scalaVersion := "2.12.10" // Adjust according to your requirements

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",

)

lazy val root = (project in file("."))
  .settings(
    name := "ETL Practice"
  )