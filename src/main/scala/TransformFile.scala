package com.hulu

import org.apache.spark.sql.SparkSession

object TransformFile {
  def main(args: Array[String]): Unit = {

    val spark : SparkSession = SparkSession.
      builder().
      master("local[*]").
      appName("SparkScala").
      getOrCreate()
    val df = ReaderFile.readData(spark)
    df.where("Age<40").show()
  }
}
