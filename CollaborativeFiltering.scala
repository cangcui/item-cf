package com.tencent.ieg.tgp.recommend

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StringType, UserDefinedType}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by lambdachen on 16/7/17.
  */

case class Preference[T](src: T, dst: T, rating: Double)
case class Similarity[T](src: T, dst: T, similarity: Double)

class Neighbour[T](val dst: T, val similarity: Double)
  extends Ordered[Neighbour[T]] with Serializable
{
  def compare(that: Neighbour[T]): Int = that.similarity.compareTo(similarity)
  override def toString = dst.toString + "," + similarity.toString
  override def hashCode(): Int = this.toString.hashCode
  override def equals(other: Any): Boolean = this.toString.equals(other.toString)
}

class NeighbourDataType[T: ClassTag] extends UserDefinedType[Neighbour[T]]{
  override def sqlType: DataType = StringType
  override def pyUDT: String = "python_null"
  override def serialize(obj: Any) = {
    obj.toString
  }
  override def deserialize(datum: Any) = {
    val Array(dst, sim) = datum.toString.split(",")
    new Neighbour(dst.asInstanceOf[T], sim.toDouble)
  }

  override def userClass: Class[Neighbour[T]] = classOf[Neighbour[T]]
  override def asNullable: NeighbourDataType[T] = this
}
case object NeighbourDataType extends NeighbourDataType

/**
  *
  * @param neighbourNumber
  */
class CollaborativeFiltering private(private var neighbourNumber: Int,
                                     private var distanceType: String) extends Serializable with Logging{

  def this() = this(10, CollaborativeFiltering.COSINE)

  def setNeighbourNumber(num: Int): this.type = {
    this.neighbourNumber = num
    this
  }

  def run[T: ClassTag](prefs: RDD[Preference[T]]): CollaborativeFilteringModel[T] = {
    val aggregatePrefs = prefs.map(p => (p.src, ArrayBuffer((p.dst, p.rating))))
      .reduceByKey(_ ++ _).cache()
    val otherAggregatePrefs = aggregatePrefs

    val cartesianPres = aggregatePrefs.cartesian(otherAggregatePrefs).filter{
      case (v1, v2) => v1._1 != v2._1
    }
    val distanceFunction = getDistanceFunction[T]
    val neighbours = cartesianPres.map{
      case (v1, v2) =>
        val sim = distanceFunction(v1, v2)
        (sim.src, Array(new Neighbour(sim.dst, sim.similarity)))
    }.reduceByKey(_ ++ _).map{
      case (src, arr) =>
        val takeNum = math.min(this.neighbourNumber, arr.size)
        (src, arr.sorted.take(takeNum))
    }

    new CollaborativeFilteringModel[T](this.neighbourNumber, neighbours)
  }

  private def getDistanceFunction[T: ClassTag]() = {
    this.distanceType match {
      case CollaborativeFiltering.COSINE =>
        cosineBasedSimilarity[T]_
    }
  }

  private def cosineBasedSimilarity[T: ClassTag](
     vectorA: (T, ArrayBuffer[(T, Double)]),
     vectorB: (T, ArrayBuffer[(T, Double)])): Similarity[T] = {
    val aSet = vectorA._2.map(_._1).toSet
    val bSet = vectorB._2.map(_._1).toSet
    val interSet = aSet.intersect(bSet)
    val s = interSet.size.toDouble / math.sqrt(aSet.size.toDouble * bSet.size.toDouble)
    Similarity(vectorA._1, vectorB._1, s)
  }
}

/**
  *
  */
object CollaborativeFiltering{

  private val COSINE = "cosine"

  def train[T: ClassTag](ratings: RDD[Preference[T]], neighbourNumber: Int): CollaborativeFilteringModel[T] = {
    //new ItemCF(neighbourNumber).run(ratings)
    null
  }
}

object MainTest{
  def main(args: Array[String]): Unit ={
//    val vectorA = (1, ArrayBuffer((2, 1.0), (3, 1.0)))
//    val vectorB = (2, ArrayBuffer((2, 1.0), (3, 1.0)))

    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val testRDD = sc.makeRDD(Seq(
      Preference(1, 2, 1.0), Preference(1, 3, 1.0),
      Preference(2, 2, 1.0), Preference(2, 3, 1.0),
      Preference(3, 4, 0.5), Preference(3, 3, 0.1)))
    val cf1 = new CollaborativeFiltering()
    val cf_model = cf1.run(testRDD)
    CollaborativeFilteringModel.SaveLoadV0_1.save(cf_model.asInstanceOf[CollaborativeFilteringModel[Int]], "E:\\code\\spark\\models")
  }
}
