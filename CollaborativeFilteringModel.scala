package com.tencent.ieg.tgp.recommend

import com.tencent.ieg.tgp.common.CommonUtility
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataType, StringType, _}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
  * Created by lambdachen on 16/7/17.
  */
class CollaborativeFilteringModel[T](
    val neighboursNum: Int,
    val neighboursInformation: RDD[(T, Array[Neighbour[T]])])
  extends Saveable with Serializable with Logging {

  require(neighboursNum > 0)
  validateNeighbours("neighbours", neighboursInformation)

  protected override val formatVersion: String = "0.1"

  private def validateNeighbours(name: String,
                                 neighbours: RDD[(T, Array[Neighbour[T]])]): Unit ={
    if(neighbours.partitioner.isEmpty){
      logWarning(s"$name factor does not have a partitioner. "
        + "Prediction on individual records could be slow.")
    }
    if(neighbours.getStorageLevel == StorageLevel.NONE){
      logWarning(s"$name factor is not cached. Prediction could be slow.")
    }
  }

  override def save(sc: SparkContext, path: String): Unit ={

  }
}

object CollaborativeFilteringModel extends ExtModel{

  object SaveLoadV0_1{
    private val thisFormatVersion = "0.1"
    val thisClassName = "CollaborativeFilteringModel"

    def save(model: CollaborativeFilteringModel[_], path: String): Unit ={
      val sc = model.neighboursInformation.sparkContext
      val sqlContext = new SQLContext(sc)
      val metadata = compact(render(
        ("class" -> thisClassName) ~
          ("Version" -> thisFormatVersion) ~
          ("neighboursNumber" -> model.neighboursNum)
      ))
      val neighbours = model.neighboursInformation
      CommonUtility.deletePath(metadataPath(path))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))
      val neighbourRows = neighbours.map(t => Row(t._1, t._2))
      val schema = buildNgbSchema(neighbourRows.take(1)(0))
      CommonUtility.deletePath(neighboursPath(path))
      sqlContext.createDataFrame(neighbourRows, schema).write.parquet(neighboursPath(path))
    }

    private def buildNgbSchema(row: Row): StructType = {
      val srcType = CommonUtility.constructFieldDataType(row(0))
      StructType(Seq(
        StructField("src", srcType, true),
        StructField("neighbours", ArrayType(NeighbourDataType), true)
      ))
    }

    private def neighboursPath(path: String): String = {
      new Path(dataPath(path), "neighbours").toUri.toString
    }
  }
}
