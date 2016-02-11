import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

object CalculateWordRelevance {
  /*
   * args(0): file name
   * args(1): text csv position
   * args(2): relevance csv position
   * args(3): ignore first line
   */
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("CalculateWordRelevance"))

    // AWS S3 Credentials
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "YOUR_ACCESS_KEY_ID")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET_ACCESS_KEY_ID")

    var inputFile = sc.textFile("s3n://bucket/calculate_word_relevance/in/" + args(0))

    if(args(3).nonEmpty)
      inputFile = inputFile.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    inputFile.flatMap(line => {
      try {
        val lineParsed = new CSVReader(new StringReader(line), ';', '"').readNext()

        lineParsed(args(1).toInt - 1)

          // Clean text
          .replaceAll("[.\t\r\n'.,;:\\*+&-/¡!¿?#%\")(]", " ") // Remove specific characters
          .replaceAll("\\b\\w{1,3}\\b"," ")                   // Remove words with length <= 3
          .replaceAll(" +", " ")
          .trim()

          // Split text in words
          .split(" ")
          .map(word => (word, lineParsed(args(2).toInt - 1).toInt))
      } catch {
        case e: Exception => {
          println("Error processing the line: " + line)
          None
        }
      }
    }).reduceByKey(_ + _)
      .sortBy(line => -line._2)
      .map(line => Array(line._1, line._2).mkString(";"))                   // Convert to CSV format
      .repartition(1)                                                       // Output in a single file
      .saveAsTextFile("s3n://bucket/calculate_word_relevance/out/" + args(0) + "_result")
  }
}
