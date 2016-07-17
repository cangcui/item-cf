import org.apache.spark.Logging
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
  * Created by lambdachen on 16/7/17.
  */

case class Similarity[T](dstPeer: T,
                         similarity: Double)

/**
  *
  * @param neighbourNumber
  */
class ItemCF private (private var neighbourNumber: Int) extends Serializable with Logging{

  def this() = this(10)

  def setSimilarNum(num: Int): this.type = {
    this.neighbourNumber = num
    this
  }

  def run(ratings: RDD[Rating]): ItemCFModel = {

    null
  }

}

/**
  *
  */
object ItemCF{
  def train(ratings: RDD[Rating], neighbourNumber: Int): ItemCFModel = {
    new ItemCF(neighbourNumber).run(ratings)
  }
}
