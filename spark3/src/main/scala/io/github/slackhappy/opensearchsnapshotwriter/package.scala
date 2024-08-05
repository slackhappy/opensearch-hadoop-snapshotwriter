package io.github.slackhappy

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

package object opensearchsnapshotwriter {

  implicit def sparkDataFrameFunctions(df: DataFrame)  = new SparkDataFrameFunctions(df)

  // TODO: more implicits

  class SparkDataFrameFunctions(df: DataFrame) extends Serializable {
    def snapshotForOpenSearch(outputPath: String, indexName: String, mappingJson: String, numShards: Int): Unit = {
      new SparkRowOpenSearchSnapshotWriter(df.rdd).snapshotForOpenSearch(
        outputPath, indexName, mappingJson, numShards
      )
    }
  }
}