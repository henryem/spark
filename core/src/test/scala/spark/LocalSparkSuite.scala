package spark

import org.scalatest.FunSuite
import akka.dispatch.Await
import akka.util.Duration
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.io.ObjectOutputStream
import java.io.IOException
import org.scalatest.BeforeAndAfter

class LocalSparkSuite extends FunSuite with ShouldMatchers with BeforeAndAfter {
  var sc: SparkContext = _
  
  def after() {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }
  
//  test("Use LocalSpark locally") {
//    val data = (0 until 1000)
//    val transform = {(num: Int) => num*2}
//    val results = Await.result(LocalSpark.runLocally(data, {rdd: RDD[Int] => rdd.map(transform).collect}), Duration.Inf)
//    results.deep should equal (data.map(transform))
//  }
//  
//  test("Use a new thread inside a Spark job") {
//    val numTasks = 4
//    val numReps = 8
//    sc = new SparkContext("local[%d]".format(numTasks), "test")
//    val reps = sc.parallelize(0 until numReps, numTasks)
//    val data = (0 until 100)
//    val transform = {(num: Int) => num*2}
//    val results = reps
//      .map(rep => {
//        val executor = Executors.newSingleThreadExecutor()
//        val result = executor.submit(new Callable[Array[Int]]() { def call() = data.map(transform).toArray })
//        executor.shutdown()
//        result.get
//      })
//      .collect
//    results.deep should equal ((0 until numReps).map(rep => data.map(transform)))
//  }
//  
//  test("Use LocalSpark inside a local Spark job") {
//    sc = LocalSpark.createLocalContext(1)
//    runTestJobInContext(sc)
//  }
  
  /** HACK: A Spark master must be set up at localhost:7077. */
  //FIXME: Use spark-cluster[] instead
  test("Use LocalSpark inside a remote Spark job") {
    sc = new SparkContext("spark://dhcp-44-128.eecs.berkeley.edu:7077", "test", "/Users/henrym/Code/spark") //FIXME
    runTestJobInContext(sc)
  }
  
  def runTestJobInContext[D](sc: SparkContext): Unit = {
    val numTasks = 1
    val numReps = 1
    val reps = sc.parallelize(0 until numReps, numTasks)
    val data = (0 until 100)
    val transform = {(num: Int) => num*2}
    val results = reps
      .map({rep =>  Await.result(LocalSpark.runLocally(data, {rdd: RDD[Int] => rdd.map(transform).collect}), Duration.Inf)})
      .collect
    results.deep should equal ((0 until numReps).map(rep => data.map(transform)))
  }
}