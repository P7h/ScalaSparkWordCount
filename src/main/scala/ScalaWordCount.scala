import org.apache.spark
import spark._
import SparkContext._

import scala.language.postfixOps

object ScalaWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkScalaWordCount")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://localhost:9000/user/hduser/input", 1)
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    val countsCached = counts.sortByKey().cache()

    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.compare(b) * (-1)
    }
    val sortedCounts = countsCached.collect().toSeq.sortBy(_._2)

    countsCached.sortByKey().saveAsTextFile("hdfs://localhost:9000/user/hduser/output_spark_scala")
    sc.makeRDD(sortedCounts, 1).saveAsTextFile("hdfs://localhost:9000/user/hduser/output_spark_scala1")
    sc.makeRDD(countsCached.top(10), 1).saveAsTextFile("hdfs://localhost:9000/user/hduser/output_spark_scala2")
    sc stop
  }
}