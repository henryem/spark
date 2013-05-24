package spark

import org.scalatest.FunSuite
import akka.dispatch.Await
import akka.util.Duration
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.io.ObjectOutputStream
import java.io.IOException

class LocalSparkSuite extends FunSuite with ShouldMatchers {
//  test("Use LocalSpark locally") {
//    val data = (0 until 1000)
//    val transform = {(num: Int) => num*2}
//    val results = Await.result(LocalSpark.runLocally(data, {rdd: RDD[Int] => rdd.map(transform).collect}), Duration.Inf)
//    results.deep should equal (data.map(transform))
//  }
  
//  test("Use a new thread inside a Spark job") {
//    val numTasks = 4
//    val numReps = 8
//    val sc = new SparkContext("local[%d]".format(numTasks), "test")
//    val reps = sc.parallelize(0 until numReps, numTasks)
//    val data = (0 until 100)
//    val transform = {(num: Int) => num*2}
//    val results = reps
//      .map(rep => {
//        val executor = Executors.newSingleThreadExecutor() //TMP
//        val result = executor.submit(new Callable[Array[Int]]() { def call() = data.map(transform).toArray })
//        executor.shutdown()
//        result.get
//      })
//      .collect
//    results.deep should equal ((0 until numReps).map(rep => data.map(transform)))
//    sc.stop()
//  }
  
  test("Use LocalSpark inside a Spark job") {
    val numTasks = 1
    val numReps = 1
    val sc = LocalSpark.createLocalContext(1)
    val reps = sc.parallelize(0 until numReps, numTasks)
    val data = (0 until 100)
    val results = reps
      .map(new OuterMapper(data))
      .collect
    val transform = {(num: Int) => num*2}
    results.deep should equal ((0 until numReps).map(rep => data.map(transform)))
    sc.stop()
  }
}

//TMP
class OuterMapper(data: Seq[Int]) extends Function1[Int,Array[Int]] with Serializable {
  override def apply(rep: Int) = {
    println("Rep=%d".format(rep)) //TMP
    val transformRdd = new Function1[RDD[Int], Array[Int]]{
      def apply(rdd: RDD[Int]) = rdd.map(InnerMapper).collect
      override def toString() = "transformRdd"
    }
    val results = Await.result(LocalSpark.runLocally(data, transformRdd), Duration.Inf)
    results
  }
      
  override def toString() = "outerMapper"
}

//TMP
object InnerMapper extends Function1[Int,Int] with Serializable {
  override def apply(num: Int) = num*2
  override def toString() = "transformRow"
}