package spark

import spark.SparkContext._

object BroadcastRddFunctions {
  implicit def rddToBroadcastRddFunctions[D: ClassManifest](rdd: RDD[D]) = new BroadcastRddFunctions[D](rdd)
  
  private def makeRepeatedBroadcastRdd[B](broadcastRdd: RDD[B], numReps: Int): RDD[(Int, Seq[B])] = {
    broadcastRdd
      .glom
      .flatMap({bcastPartition =>
        val bcastPartitionSeq = bcastPartition.toSeq
        (0 until numReps).map(baseRddPartitionIdx => (baseRddPartitionIdx, bcastPartitionSeq))
      })
  }
  
  private def glomWithIndex[D](rdd: RDD[D]): RDD[(Int, Array[D])] = {
    rdd
      .glom
      .mapPartitionsWithIndex({(idx: Int, part: Iterator[Array[D]]) => Iterator.single((idx, part.next))})
  }
  
  private class IdentityPartitioner(private val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any) = {
      require(key.isInstanceOf[Int])
      val keyInt = key.asInstanceOf[Int]
      require(keyInt < numPartitions)
      keyInt
    }
    
    override def equals(other: Any): Boolean = other match {
      case h: IdentityPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
  }
}

class BroadcastRddFunctions[D: ClassManifest](baseRdd: RDD[D]) extends Serializable {
  def withBroadcast[B](broadcastRdd: RDD[B]): RDD[(D, Seq[B])] = {
    val numBaseRddPartitions = baseRdd.partitions.size
    val numBroadcastRddPartitions = broadcastRdd.partitions.size
    val partitioner = new BroadcastRddFunctions.IdentityPartitioner(numBaseRddPartitions)
    BroadcastRddFunctions.glomWithIndex(baseRdd)
      .cogroup(BroadcastRddFunctions.makeRepeatedBroadcastRdd(broadcastRdd, numBaseRddPartitions), partitioner)
      .flatMap({case (baseRddPartitionIdx, (baseRddPartitionSingleton, bcastPartitions)) =>
        require(baseRddPartitionSingleton.size == 1)
        require(bcastPartitions.size == numBroadcastRddPartitions)
        val bcastRows = bcastPartitions.flatten
        baseRddPartitionSingleton.head.map(baseRddRow => (baseRddRow, bcastRows))
      })
  }
  
  def withSingletonBroadcast[B](broadcastRdd: RDD[B]): RDD[(D, B)] = {
    val numBaseRddPartitions = baseRdd.partitions.size
    val numBroadcastRddPartitions = broadcastRdd.partitions.size
    val partitioner = new BroadcastRddFunctions.IdentityPartitioner(numBaseRddPartitions)
    BroadcastRddFunctions.glomWithIndex(baseRdd)
      .cogroup(BroadcastRddFunctions.makeRepeatedBroadcastRdd(broadcastRdd, numBaseRddPartitions), partitioner)
      .flatMap({case (baseRddPartitionIdx, (baseRddPartitionSingleton, bcastPartitions)) =>
        require(baseRddPartitionSingleton.size == 1)
        require(bcastPartitions.size == numBroadcastRddPartitions)
        val bcastRows = bcastPartitions.flatten.seq
        require(bcastRows.size == 1)
        baseRddPartitionSingleton.head.map(baseRddRow => (baseRddRow, bcastRows(0)))
      })
  }
}