package spark

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import spark.BroadcastRddFunctions._
import spark.SparkContext._

class BroadcastRddFunctionsSuite extends FunSuite with ShouldMatchers {
  test("simple test of broadcast functionality") {
    LocalSparkContext.withSpark(new SparkContext("local[4]", "test"))({sc => 
      val baseData = sc.parallelize(0 until 10000, 10)
      val broadcastData = sc.parallelize(0 until 100, 5)
      val result = baseData
        .withBroadcast(broadcastData)
        .map({case (baseRow, broadcastData) => baseRow + broadcastData.sum})
        .collect
      result.deep should equal ((0 until 10000).map(_ + (0 until 100).sum))
    })
  }
  
  test("iterate with singleton broadcasts") {
    LocalSparkContext.withSpark(new SparkContext("local[4]", "test"))({sc => 
      val baseDataCeil = 100
      val trueSlope = 0.01
      val baseDataLocal = (0 until baseDataCeil).map(_.toDouble).zip((0L until baseDataCeil).map(_*trueSlope))
      val baseData: RDD[(Double, Double)] = sc.parallelize(baseDataLocal, 10)
      val numReps = 20
      val baseStepSize: Double = 0.1
      var currentParameters: RDD[Double] = sc.parallelize(Iterator(0.0).toSeq, 1)
      for (i <- 1 to numReps) {
        val stepSize = baseStepSize / i
        // Gradient descent for least-squares linear regression.
        currentParameters = baseData
          .withSingletonBroadcast(currentParameters)
          .map({case ((x, y), currentSlope) => 
            val localGradient = 2*(y - x*currentSlope)
            (0, (1L, localGradient, currentSlope))
          })
          .reduceByKey({(gradient1: (Long, Double, Double), gradient2: (Long, Double, Double)) => 
            val count1 = gradient1._1
            val count2 = gradient2._1
            val newCount = count1 + count2
            val avgGradient = gradient1._2*(count1/newCount.toDouble) + gradient2._2*(count2/newCount.toDouble)
            val currentSlope = gradient1._3
            (newCount, avgGradient, currentSlope)
          })
          .map({case (key, (count, avg, currentSlope)) =>
            currentSlope + stepSize*avg
          })
      }
      val finalParameters = currentParameters.collect
      finalParameters.size should be (1)
      finalParameters(0) should be (trueSlope plusOrMinus .1*trueSlope)
    })
  }
}