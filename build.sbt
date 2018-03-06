lazy val phoneCompany = (project in file(".")).settings(
  Seq(
    name := "disco-test-phone-company",
    version := "1.0",
    scalaVersion := "2.11.4"
  )
)

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)
