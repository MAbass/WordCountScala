import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]): Unit = {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SparkWordCount"))

    val data = sc.textFile("C:\\Users\\Abass\\Desktop\\MIABD1\\Pyspark\\wordcount\\input\\HadoopPoem0.txt");

    val splitdata = data.flatMap(line => line.split(" "));
    val reduce = splitdata.map(word => (word,1)).reduceByKey(_+_)

    println(reduce.collect.mkString("Array(", ", ", ")"))
  }
}