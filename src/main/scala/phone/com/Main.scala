package phone.com

import org.apache.spark.sql.{Encoders, Row, SparkSession}

case class PhoneLogSchema(CustomerId: String, PhoneNumber: String, CallDuration: String)

class LogParser(spark: SparkSession) extends java.io.Serializable {

  def process(resourceName: String): Array[Row] = {
    val sqlContext = spark.sqlContext
    val df = sqlContext
      .read
      .format("csv")
      .option("delimiter", " ")
      .schema(Encoders.product[PhoneLogSchema].schema)
      .load(resourceName)

    df.createOrReplaceTempView("phone_log")
    val sqlStmt = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/sql1.sql")).getLines.mkString
    val dfCost = sqlContext.sql(sqlStmt)
    val output = dfCost.collect()
    output
  }

}


object Main extends App {
  val spark = SparkSession
    .builder()
    .appName("PhoneCompany")
    .master("local")
    .getOrCreate()
  val parser = new LogParser(spark)
  val output = parser.process("src/main/resources/calls.log")
  spark.stop()
  println("Call Cost(pence) per customer report:")
  output.foreach(x => println("%s %s".format(x.get(0), x.get(1))))

}
