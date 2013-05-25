package spark

import akka.dispatch.Await
import akka.util.Duration

object LocalSparkTestRunner {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val numTasks = args(1).toInt
    val numReps = args(2).toInt
    val dataSize = args(3).toInt
    
    val sc = new SparkContext(master, "test")
    val reps = sc.parallelize(0 until numReps, numTasks)
    val data = (0 until dataSize)
    val transform = {(num: Int) => num*2}
    val results = reps
      .map({rep =>  Await.result(LocalSpark.runLocally(data, {rdd: RDD[Int] => rdd.map(transform).collect}), Duration.Inf)})
      .collect
    println("Results: %s".format(results.deep))
    sc.stop()
    System.exit(0)
  }
}