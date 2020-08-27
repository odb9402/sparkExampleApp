import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDsampleApp {
  def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("RDDsampleApp").setMaster("local")
        val sc = new SparkContext(conf)
        
        //Parallelized Collections
        val arrData = Array(1,2,3,4,5)
        val distArrData = sc.paralleize(arrData)
        println(s"Sum arr 1 to 5 :[${distArrData.reduce((a, b) => a + b)}]")

        //Using External datasets
        val distFile = sc.textFile("sample_data.txt")
  }
}