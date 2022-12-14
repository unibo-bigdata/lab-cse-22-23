import java.io.InputStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

// spark-submit --class ExerciseNetcat BD-305-streaming-spark.jar <exerciseNumber> <host> <port>
object ExerciseNetcatSolutions extends App {

  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Exercise 305 - Spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Load S3 credentials
    val stream: InputStream = getClass.getResourceAsStream("/aws_credentials.txt")
    val lines = scala.io.Source.fromInputStream( stream ).getLines.toList

    // Create an RDD from the files in the given folder
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines(0))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))

    if(args.length >= 3){
      val host = args(1)
      val port = args(2).toInt
      val path = if (args.length >= 4) args(3) else ""
      args(0) match {
        case "1" => exercise1(spark.sparkContext,host,port)
        case "2" => exercise2(spark.sparkContext,host,port)
        case "3" => exercise3(spark.sparkContext,host,port, path)
        case "4" => exercise4(spark.sparkContext,host,port, path)
        case "5" => exercise5(spark.sparkContext,host,port)
        case "6" => exercise6(spark.sparkContext,host,port)
        case "7" => exercise7(spark.sparkContext,host,port, path)
        case "8" => exercise8(spark.sparkContext,host,port, path)
      }
    }
  }

  /**
   * Simple total count of words
   * Words are detected by splitting lines on spacing (" ")
   * @param sc
   */
  def exercise1(sc: SparkContext, host: String, port: Int): Unit = {
    val ssc = new StreamingContext(sc, Seconds(3))
    val lines = ssc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val count = words.count()
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Word count
   * Words are detected by splitting lines on spacing (" ")
   * @param sc
   */
  def exercise2(sc: SparkContext, host: String, port: Int): Unit = {
    val ssc = new StreamingContext(sc, Seconds(3))
    val lines = ssc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Enabling checkpoint: this allows the application to restart from where it last stopped
   *
   * NOTICE: you need to create a directory on S3 to store the checkpoint data
      * - s3a://unibo-bd2223-egallinucci/streaming/cp3
   * @param sc
   */
  def exercise3(sc: SparkContext, host: String, port: Int, path: String): Unit = {
    def functionToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(sc, Seconds(3))
      val lines = newSsc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
      wordCounts.print()
      newSsc.checkpoint(path)
      newSsc
    }

    val ssc = StreamingContext.getOrCreate(path, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Enabling state: this allows the job to continuously update a temporary result, i.e., the state
   * NOTICE: you need to either
   * - create a DIFFERENT directory on S3 to store the checkpoint data
   * - empty the previous directory
   * Otherwise, the application will re-run the job already checkpointed in the directory
   * @param sc
   */
  def exercise4(sc: SparkContext, host: String, port: Int, path: String): Unit = {
    def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
      Some(oldValue.getOrElse(0) + newValues.sum)
    }

    def functionToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(sc, Seconds(3))
      val lines = newSsc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.flatMap(_.split(" "))
      val cumulativeWordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunction)
      cumulativeWordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
      newSsc.checkpoint(path)
      newSsc
    }

    val ssc = StreamingContext.getOrCreate(path, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * This job carries out word counting on a sliding window that is wide 30 seconds and is updated every 3 seconds.
   * @param sc
   */
  def exercise5(sc: SparkContext, host: String, port: Int): Unit = {
    val ssc = new StreamingContext(sc, Seconds(3))
    val lines = ssc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(30), Seconds(3))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * The dataset for this exercise is the content of dataset/tweet.dsv
   * This job is a simple evolution of word counting to see the currently trending hashtags.
   * The window is wide 1 minute and it is updated every 5 seconds.
   * @param sc
   */
  def exercise6(sc: SparkContext, host: String, port: Int): Unit = {
    val ssc = new StreamingContext(sc, Seconds(1))
    val lines = ssc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(60), Seconds(5))
    val tweets = lines.filter(_.nonEmpty).map(_.split("\\|"))
    val hashtags = tweets.map(x => x(2)).flatMap(_.split(" ")).filter(_.nonEmpty)
    val hashtagCounts = hashtags.map(x => (x, 1)).reduceByKey(_ + _)
    hashtagCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * This is a stateful job to incrementally count the number of tweets by city.
   * Remember to either create a new directory on S3 or to empty the previous one.
   * @param sc
   */
  def exercise7(sc: SparkContext, host: String, port: Int, path: String): Unit = {
    def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
      Some(oldValue.getOrElse(0) + newValues.sum)
    }

    def functionToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(sc, Seconds(3))
      val lines = newSsc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
      val tweets = lines.filter(_.nonEmpty).map(_.split("\\|"))
      val cities = tweets.filter(x => x(4)!="" && x(4)!="0").map(x => x(4))
      val cumulativeCityCounts = cities.map(x => (x, 1)).updateStateByKey(updateFunction)
      cumulativeCityCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
      newSsc.checkpoint(path)
      newSsc
    }

    val ssc = StreamingContext.getOrCreate(path, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * This job extends the previous one by calculating also the average sentiment (per country instead of per city).
   * Remember to either create a new directory on S3 or to empty the previous one.
   * @param sc
   */
  def exercise8(sc: SparkContext, host: String, port: Int, path: String): Unit = {
    def updateFunction( newValues: Seq[(Int,Int)], oldValue: Option[(Int,Int,Int,Double)] ): Option[(Int,Int,Int,Double)] = {
      val oldValue2 = oldValue.getOrElse(0,0,0,0.0)
      val totTweets = oldValue2._1 + newValues.map(_._1).sum
      val totSentiment = oldValue2._2 + newValues.map(_._2).sum
      val countSentiment = oldValue2._3 + newValues.size
      val avgSentiment = totSentiment.toDouble/countSentiment.toDouble
      Some((totTweets, totSentiment, countSentiment, avgSentiment))
    }

    def functionToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(sc, Seconds(3))
      val lines = newSsc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
      val tweets = lines.filter(_.nonEmpty).map(_.split("\\|"))
      val cities = tweets.filter(x => x(7)!="" && x(7)!="0" && x(3)!="").map(x => ( x(7),(1,x(3).toInt) ) )
      val cumulativeCityCounts = cities.updateStateByKey(updateFunction)
      cumulativeCityCounts
        .mapValues(v => (v._1, v._2, v._3, Math.round(v._4*100).toDouble/100))
        .map({case(k,v)=>(v,k)})
        .transform({ rdd => rdd.sortByKey(false) })
        .print()
      newSsc.checkpoint(path)
      newSsc
    }

    val ssc = StreamingContext.getOrCreate(path, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }

}