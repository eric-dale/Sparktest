package com.hulu

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReaderFile {
  def main(args: Array[String]): Unit = {

    val spark : SparkSession = SparkSession.
      builder().
      master("local[*]").
      appName("SparkScala").
      getOrCreate()

val df = readData(spark)
    df.printSchema()
  }
  def readData(spark : SparkSession): DataFrame = {
    val df = spark
      .read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("src/main/resources/data/sample.csv")
    return df
  }
}

