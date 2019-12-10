package com.mrmrmr.hdfs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ExpediaReader {
  def readFromHdfs(session: SparkSession, master: String, path: String) : (DataFrame, DataFrame) = {
    val window_srch_ci = Window
      .partitionBy("hotel_id")
      .orderBy("srch_ci", "srch_co")

    session.sqlContext
      .sparkContext
      .hadoopConfiguration
      .set("avro.mapred.ignore.inputs.without.extension", "true")

    val df =session.read
      .format("com.databricks.spark.avro")
      .load(master + path)
      .withColumnRenamed("id", "expedia_id")
      .select("hotel_id", "expedia_id", "srch_ci", "srch_co")
      .withColumn("date_diff_col",
        datediff(col("srch_ci"), lag("srch_ci", 1).over(window_srch_ci)))

    val invalid_df = df.where((col("date_diff_col") > 1) && (col("date_diff_col") < 31))
    val valid_df   = df.where((col("date_diff_col") < 2) || (col("date_diff_col") > 30))

    (valid_df, invalid_df)
  }
}