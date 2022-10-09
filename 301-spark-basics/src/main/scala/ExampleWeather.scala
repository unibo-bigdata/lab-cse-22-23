import org.apache.spark.sql.SparkSession

import java.io.InputStream
import scala.io.Source._

// spark-submit --class ExampleWeather BD-301-spark-basics.jar \\
//    s3a://unibo-bd2122-egallinucci/datasets/weather-sample1.txt s3a://unibo-bd2122-egallinucci/spark/301-2
object ExampleWeather extends App {

  // Function to parse weather records; returns key -value pairs in the form(month, temperature)
  def parseWeatherLine(line: String): (String, Double) = {
    val year = line.substring(15, 19)
    val month = line.substring(19, 21)
    val day = line.substring(21, 23)
    var temp = line.substring(87, 92).toInt
    (month, temp / 10)
  }

  override def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ExampleWeather Spark").getOrCreate()

    // Setup default parameters
    val inputDir = args(0)
    val outputDir = args(1)

    // Load S3 credentials
    val stream: InputStream = getClass.getResourceAsStream("/aws_credentials.txt")
    val lines = scala.io.Source.fromInputStream( stream ).getLines.toList

    // Create an RDD from the files in the given folder
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines(0))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))
    val rddWeather = spark.sparkContext.textFile(inputDir)

    //Parse records
    val rddWeatherKv = rddWeather.map(x => parseWeatherLine(x))
    //Aggregate by key(i.e., month) to compute the sum and the count of temperature values
    val rddTempDataPerMonth = rddWeatherKv.aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
    //Calculate the average temperature in each record and sort the result by month
    val rddResult = rddTempDataPerMonth.coalesce(1).map({ case (k, v) => (k, v._1 / v._2) }).sortByKey()

    //Save the RDD on HDFS; the directory should NOT exist
    rddResult.saveAsTextFile(outputDir)
  }

}