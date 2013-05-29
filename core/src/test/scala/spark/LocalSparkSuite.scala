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
  
  test("Use LocalSpark locally, not inside any Spark operations") {
    val data = (0 until 1000)
    val transform = {(num: Int) => num*2}
    val results = Await.result(LocalSpark.runLocally(data, {rdd: RDD[Int] => rdd.map(transform).collect}), Duration.Inf)
    results.deep should equal (data.map(transform))
  }
  
  test("Use LocalSpark to create and collect an RDD inside a local Spark job using a single thread") {
    val sc = new SparkContext("local", "test")
    LocalSparkContext.withSpark(sc)(makeSimpleTestJob())
  }
  
  test("Use LocalSpark to create and collect an RDD inside a local Spark job using multiple threads") {
    val sc = new SparkContext("local[4]", "test")
    LocalSparkContext.withSpark(sc)(makeSimpleTestJob())
  }
  
  test("Use LocalSpark inside a remote Spark job with several workers") {
    val sc = new SparkContext("local-cluster[4,2,512]", "test")
    LocalSparkContext.withSpark(sc)(makeSimpleTestJob())
  }
  
  test("Use LocalSpark inside a remote Spark job with one worker") {
    val sc = new SparkContext("local-cluster[1,2,512]", "test")
    LocalSparkContext.withSpark(sc)(makeSimpleTestJob())
  }
  
  test("Use LocalSpark inside a remote Spark job with one worker running one thread") {
    val sc = new SparkContext("local-cluster[1,1,512]", "test")
    LocalSparkContext.withSpark(sc)(makeSimpleTestJob())
  }
  
  private def makeSimpleTestJob(): SparkContext => Unit = {
    {sc =>
      val numTasks = 10
      val numReps = 10
      val reps = sc.parallelize(0 until numReps, numTasks)
      val data = (0 until 100)
      val transform = {(num: Int) => num*2}
      val results = reps
        .map({_ => Await.result(LocalSpark.runLocally(data, {rdd: RDD[Int] => rdd.map(transform).collect}), Duration.Inf)})
        .collect
      results.deep should equal ((0 until numReps).map(rep => data.map(transform)))
    }
  }
  
  test("Use LocalSpark nested inside further LocalSpark jobs") {
    val data = (0 until 10)
    val transform = {(num: Int) => num*2}
    val numReps = 2
    val numNestingLevels = 4
    
    def runNestedJob(nestingLevel: Int): Any = {
      if (nestingLevel == 0) {
        data.map(transform)
      } else {
        Await.result(
          LocalSpark.runLocally(
            (0 until numReps),
            {rdd: RDD[_] => rdd.map(_ => runNestedJob(nestingLevel-1)).collect}),
          Duration.Inf)
      }
    }
    
    def makeExpectedResult(nestingLevel: Int): Any = {
      if (nestingLevel == 0) {
        data.map(transform)
      } else {
        (0 until numReps).map(rep => makeExpectedResult(nestingLevel-1))
      }
    }
    
    val results = runNestedJob(numNestingLevels)
    results.asInstanceOf[Array[_]].deep should equal (makeExpectedResult(numNestingLevels))
  }
}