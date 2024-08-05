package io.github.slackhappy.opensearchsnapshotwriter.example

import io.github.slackhappy.opensearchsnapshotwriter._
import org.apache.spark.sql.SparkSession

case class FD (_id: String, docId: Long, title: String, text: String)

object SimpleExampleJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val dfData = (1 to 10000).map(id => FD("id" + id, id, "title" + id, "text" + id))
    val df = spark.createDataFrame(dfData)
    val mapping =
      """
       {"properties":{
          "docId": {"type": "long"},
          "title": {"type": "text"},
          "text": {"type": "text"}
        }}
      """
    df.snapshotForOpenSearch("s3a://spark-data/snapshots", "test", mapping, 2)
    spark.stop()
  }
}