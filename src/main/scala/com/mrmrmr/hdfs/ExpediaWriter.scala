package com.mrmrmr.hdfs

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, dayofmonth, month, year}

object ExpediaWriter {
  def writeToHdfs(df: DataFrame, hdfsPath: String): Unit = {
    df
      .withColumn("srch_ci_year", year(col("srch_ci")))
      .withColumn("srch_ci_month", month(col("srch_ci")))
      .withColumn("srch_ci_day", dayofmonth(col("srch_ci")))
      .drop("srch_ci")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("srch_ci_year")
      .format("com.databricks.spark.avro")
      .save(hdfsPath)
  }
}
