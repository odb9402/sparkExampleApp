package app

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import app.singletonFunc 
import scala.reflect.io.Directory
import java.io.File

/*
Note that object is the STATIC class (singleton) and
  class is just a normal class.
*/
object RDDsampleApp {
  val nline = "\n"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDsampleApp").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")

    /*
    1. Parallelized Collections
    
    Spark or Java normal collections can be transform into paralleized RDD
    */
    val arrData = Array(1,2,3,4,5)
    val distArrData = sc.parallelize(arrData)
    println(s"Sum arr 1 to 5 :[${distArrData.reduce((a, b) => a + b)}]")

    /*
    2. Using External datasets
    
    Spark can use external data such as normal files and hadoop RDD as the sparkRDD.
    */
    val distFile = sc.textFile("./data/sample_data_*.txt") // Can use wildcard or a directory as the input
    //val hadoopFile = sc.hadoopRDD() 
    println(s"Total length of the file :[${distFile.map(s => s.length).reduce((a,b) => a+b)}]")
    distFile.saveAsObjectFile("./data/object_file")
    
    val distFileDir = new Directory(new File("./data/object_file"))
    distFileDir.deleteRecursively()
  
    /*
    3. RDD Operations
    
    RDDs support two operations: 1) transformations, 2) actions
    1) transformation: creat a new dataset from existing one.(lazy operation)
    2) actions: return a value from a computation on the dataset.

    */
    val lines = sc.textFile("./data/sample_data_*.txt").cache() // read sample data and store in cache
    val lineLengths = lines.map(s => s.length) // transformation
    val totalLengths = lineLengths.reduce((a,b) => a+b) // action
    lineLengths.persist() // lineLength will be saved as the cache (in-memory)

    /*
    4. Passing functions to Spark

    You can pass the function for the map operation of RDD.
    1) anoymous functions(lambda functions) (x,y)=> {x+y}
    2) static methods in a global singleton object.
    3) 
    */
    val asciiLines = lines.map(singletonFunc.removeSymbols)
    println("Filtered strings :")
    asciiLines.take(10).foreach(println)

    val asciiLines2 = lines.map(removeSymbols)
    asciiLines2.take(10).foreach(println)

    val newLineLines = concatNewline(lines)
    newLineLines.take(10).foreach(println)
  
    /*
    5. Closures (Accumulator)

    If we use Spark in a non-local mode which has a cluster, naive sum rdd.foreach(x=>counter+x)
    will behave massy.

    Prior to execution, Spark computes the task`s "closure". If the task will be computed under 
    the cluster mode, the driver node(name node?) is no longer visible to the executors. The exe
    cutors only see the copy from the serialized closure. (The final value still be zero)

    Solution: Using "Accumulator"
    */

    /*
    6. Working with key-value pairs

    "Tuple2" objects can deal with key-value operation (by writing (a, b)).

    */
    val onlyAscii = lines.map(line => removeSymbols(line))
    val splitAscii = onlyAscii.flatMap(line => line.split(" ")).map(s => (s, 1))
    val countsAscii = splitAscii.reduceByKey((a, b) => a + b) // (value1, value2) => value1 + value2
    val sortedCountsAscii = countsAscii.sortByKey()

    println("\n\nSorted by key") 
    sortedCountsAscii.take(10).foreach(println)
    
    println("\n\nSorted by value") 
    countsAscii.sortBy(x => -1*x._2).take(10).foreach(println)
  }
  
  def removeSymbols(s: String): String = {
    s.replaceAll("[^a-zA-Z ]","")
  }

  def concatNewline(rdd: RDD[String]): RDD[String] = {
    val nline_ = this.nline
    rdd.map(x => x + nline_)
  }
}