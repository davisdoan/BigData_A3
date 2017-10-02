
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.util.random
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.collection.mutable.ArrayBuffer

object SimpleScalaSpark {

  def main(args: Array[String]) {
    /**
    val logFile = "/Users/toddmcgrath/Development/spark-1.6.1-bin-hadoop2.4/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
      **/

    makeNumberFile(50000,"data.txt")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      /**
      val data = Array(1, 2, 3, 4, 5)
      val distData = sc.parallelize(data)
       distData can be operated on in parallel
      distData.reduce((a, b) => a + b)
      distData.foreach(x => {println(x)})
        **/

      /**
      val jsonFlightFile = "2015-summary.json"
      val flightData2015 = spark.read.json(jsonFlightFile)
      val elements = flightData2015.take(2)
      elements.foreach(x=> {println(x)})
        **/

      val spark = SparkSession.builder().appName("Simple Application").getOrCreate()

      // q2
      val dataFileRDD = sc.textFile("data.txt")
      val entireCollection = dataFileRDD.collect()
      val lineLengths = dataFileRDD.map( s=> s.length)
      val totalLength = lineLengths.reduce((a, b)=> a+b)
      println("total length is " + totalLength)
      val sumRDD = dataFileRDD.collect().map(x => x.toDouble)
      val summTEST = dataFileRDD.map(x=> x.toDouble)

      val meannnnnn = summTEST.mean()
      val stddev = summTEST.stdev()

      val dataFileCount = dataFileRDD.count()
      val dataFileSum = dataFileRDD.map(word => (word, 1)).reduceByKey(_+_)
      println("The count is " + dataFileCount + " the sum is " + sumRDD + " the mean is " + meannnnnn + " the std dev is " + stddev)


      // q3
      val df = spark.read.text("data.txt")
      df.select(mean(df("value"))).show
      df.select(stddev_pop(df("value"))).show
      //dataFileDF.select(stddev(dataFileDF("value"))).show
      //dataFileDF.select(stddev(dataFileDF.select("value"))).show

      //q4
      val hundredSamples = df.sample(true, .002)
      val mySum = hundredSamples.select(mean("value")).show()
      println("count is " + hundredSamples + " for sum of " + mySum)

      //q5
      makeNumberFile(100, "hundredDoubles.txt")
      val hundredDoublesRDD = sc.textFile("hundredDoubles.txt")
      val slider = hundredDoublesRDD.map(x => x.toDouble)
      val mover = slider.sliding(20).map(slice => (slice.sum / slice.size))
      mover.foreach(x=> {println(" The mean of the window is " + x)})
      //val slidingWindowRDD = hundredDoublesRDD.collect().sliding(20).map(slice => slice.toDouble)






    } finally {
      sc.stop()
    }


  }

  // q1
  def makeNumberFile(N: Int, filename: String): Unit = {
    val randomNum = scala.util.Random
    val randArray = Array.fill[Double](N)(randomNum.nextGaussian())
    val mean = randArray.sum / randArray.length
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    val sum = randArray.sum
    randArray.foreach(element => { bw.write(element.toString + "\n")})
    bw.close()
    println("sum is  " + sum + "Mean is " + mean + " with the size of " + randArray.length + " random array is " + randArray(1))

  }

}