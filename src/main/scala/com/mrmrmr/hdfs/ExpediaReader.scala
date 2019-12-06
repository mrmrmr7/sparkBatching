package com.mrmrmr.hdfs

import org.apache.spark
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import com.mrmrmr.StartHere.getListOfHDFSFiles
import com.mrmrmr.kafka.HotelWithWeatherOption
import org.apache.spark.sql.types.StructField

object ExpediaReader {
  def read(session: SparkSession, master: String, path: String) : Array[Expedia] = {
    print(master + path)
    session.sqlContext
      .sparkContext
      .hadoopConfiguration
      .set("avro.mapred.ignore.inputs.without.extension", "true")


    import session.implicits._
    val list = getListOfHDFSFiles(session, master + path)

    while (list.hasNext) {
      val nextPath = list.next().getPath
      if (!nextPath.toString.endsWith("_SUCCESS")){
        val df = session.read
          .format("com.databricks.spark.avro")
          .load(nextPath.toString)
          .withColumnRenamed("id", "expedia_id")
          .as[Expedia]
          .collect()

        for (each <- df) {
          println(each.toString)
        }
      }
    }


    new Array[Expedia](3)
  }
}

case class Expedia(expedia_id: Option[String],
                   date_time: Option[String],
                   site_name: Option[String],
                   posa_continent: Option[Int],
                   user_location_country: Option[Int],
                   user_location_region: Option[Int],
                   user_location_city: Option[Int],
                   user_id: Option[Int],
                   orig_destination_distance: Option[Double],
                   is_mobile: Option[Int],
                   is_package: Option[Int],
                   channel: Option[Int],
                   srch_ci: Option[String],
                   srch_co: Option[String],
                   srch_adults_cnt: Option[Int],
                   srch_children_cnt: Option[Int],
                   srch_rm_cnt: Option[Int],
                   srch_destination_id: Option[Int],
                   srch_destination_type_id: Option[Int],
                   hotel_id: Option[String])
