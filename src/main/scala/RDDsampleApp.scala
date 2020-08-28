import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.reflect.io.Directory
import java.io.File


object RDDsampleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDsampleApp").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")

    //Parallelized Collections
    val arrData = Array(1,2,3,4,5)
    val distArrData = sc.parallelize(arrData)
    println(s"Sum arr 1 to 5 :[${distArrData.reduce((a, b) => a + b)}]")

    //Using External datasets
    val distFile = sc.textFile("./data/sample_data_*.txt") // Can use wildcard or a directory as the input
    //val hadoopFile = sc.hadoopRDD() 
    println(s"Total length of the file :[${distFile.map(s => s.length).reduce((a,b) => a+b)}]")
    distFile.saveAsObjectFile("./data/object_file")
    
    val distFileDir = new Directory(new File("./data/object_file"))
    distFileDir.deleteRecursively()
  
          
  }
}