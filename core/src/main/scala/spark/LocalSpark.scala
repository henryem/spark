package spark

import java.util.concurrent.Executors
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import java.util.concurrent.Callable
import java.util.Properties
import scala.collection.JavaConversions
import com.google.common.base.Objects

/** 
 * Various hacks to run Spark locally, potentially within an existing Spark
 * job.
 */
object LocalSpark {
  /** Run @rddFunc locally on an RDD wrapping @data. */
  def runLocally[D: ClassManifest,R](data: Seq[D], rddFunc: RDD[D] => R): Future[R] = {
    val executor = Executors.newSingleThreadExecutor()
    val ec = ExecutionContext.fromExecutor(executor)
    Future({
      require(SparkEnv.get == null)
      val localSc = createLocalContext()
      require(SparkEnv.get == localSc.env)
      val localRdd = createLocalRdd(data, localSc)
      val result = rddFunc(localRdd)
      require(SparkEnv.get == localSc.env)
      localSc.stop()
      result
    })(ec)
  }
  
  
  def createLocalContext(numThreads: Int = 1): SparkContext = {
    this.synchronized {
      val oldSparkProperties = temporarilySetSparkProperties()
      val oldProperties = System.getProperties().clone().asInstanceOf[Properties]
      val sc = new SparkContext("local[%d]".format(numThreads), "testName") //FIXME
      val newProperties = System.getProperties().clone().asInstanceOf[Properties]
      displayDiff(oldProperties, newProperties) //TMP
      resetOldSparkProperties(oldSparkProperties)
      sc
    }
  }
  
  private def displayDiff(from: Properties, to: Properties): Unit = {
    val diff: Map[String, (Option[String], Option[String])] =
      JavaConversions.asScalaSet(from.keySet()).toSet
        .union(JavaConversions.asScalaSet(to.keySet()).toSet)
        .map(_.asInstanceOf[String])
        .flatMap(key => {
          val fromVal = from.getProperty(key, null)
          val toVal = to.getProperty(key, null)
          if (!Objects.equal(fromVal, toVal)) {
            Some((key, (Option(fromVal), Option(toVal))))
          } else {
            None
          }
        })
        .toMap
    println("Creating a SparkContext changed the following Java system properties: %s".format(diff))
  }
  
  private case class SparkProperties(host: String, port: String, fileserverUri: String)
  private val SPARK_DRIVER_HOST_PROPERTY = "spark.driver.host"
  private val SPARK_DRIVER_PORT_PROPERTY = "spark.driver.port"
  private val SPARK_FILESERVER_URI_PROPERTY = "spark.fileserver.uri"
  
  private def temporarilySetSparkProperties(): SparkProperties = {
    val oldProperties = SparkProperties(
        System.getProperty(SPARK_DRIVER_HOST_PROPERTY),
        System.getProperty(SPARK_DRIVER_PORT_PROPERTY),
        System.getProperty(SPARK_FILESERVER_URI_PROPERTY))
    setOrRemoveSystemProperty(SPARK_DRIVER_HOST_PROPERTY, "localhost")
    setOrRemoveSystemProperty(SPARK_DRIVER_PORT_PROPERTY, "0")
    setOrRemoveSystemProperty(SPARK_FILESERVER_URI_PROPERTY, null)
    oldProperties
  }
  
  private def resetOldSparkProperties(oldProperties: SparkProperties): Unit = {
    setOrRemoveSystemProperty(SPARK_DRIVER_HOST_PROPERTY, oldProperties.host)
    setOrRemoveSystemProperty(SPARK_DRIVER_PORT_PROPERTY, oldProperties.port)
    setOrRemoveSystemProperty(SPARK_FILESERVER_URI_PROPERTY, oldProperties.fileserverUri)
  }
  
  private def setOrRemoveSystemProperty(propertyName: String, value: String): Unit = {
    if (value == null) {
      System.clearProperty(propertyName)
    } else {
      System.setProperty(propertyName, value)
    }
  }
  
  def createLocalRdd[D: ClassManifest](data: Seq[D], localSc: SparkContext): RDD[D] = {
    val localRdd = localSc.parallelize(data, 1)
    localRdd
  }
}