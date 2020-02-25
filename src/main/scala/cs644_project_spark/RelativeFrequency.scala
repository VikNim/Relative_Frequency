package cs644_project_spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RelativeFrequency {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSQLScalaRelativeFrequency")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val neighborWindow = 2
    val input = args(0)
    val output = args(1)

    val broadcastWindow = sc.broadcast(neighborWindow)

    val rawData = sc.textFile(input)

    val rfSchema = StructType(Seq(
      StructField("word", StringType, false),
      StructField("neighbor", StringType, false),
      StructField("frequency", IntegerType, false)
    ))

    val rowRDD = rawData.flatMap(line => {
      val tokens = line.split("\\s")
      for
      {
        i <- 0 until tokens.length
        start = if (i - broadcastWindow.value < 0) 0 else i - broadcastWindow.value
        end = if (i + broadcastWindow.value >= tokens.length) tokens.length - 1 else i + broadcastWindow.value
        j <- start to end if (j != i)
      } yield Row(tokens(i), tokens(j), 1)
    })

    val rfDataFrame = spark.createDataFrame(rowRDD, rfSchema)
    rfDataFrame.createOrReplaceTempView("rfTable")

    import spark.sql

    val query = "select a.word, a.neighbor, (a.feq_total/b.total) rf " +
      "from (select word, neighbor, sum(frequency) feq_total from rfTable group by word, neighbor) a " +
      "inner join (select word, sum(frequency) total from rfTable group by word) b on a.word = b.word"

    val sqlResult = sql(query)

    sqlResult.show()

    sqlResult.rdd.saveAsTextFile(output + "/textFormat")

    spark.stop()
  }
}

