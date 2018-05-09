package org.frb.bronze.process

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import commonregex._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.frb.bronze.process._
import org.apache.spark.sql.types._

object fileCheck {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val fileType : String = args(0)
    val fileLocation : String = args(1)
    val tableName : String = args(2)
    val userName : String = args(3)
    val timeStampWrite : Long = args(4).toLong

    val inputDF = spark.read.format(fileType).option("header","true").option("inferSchema","true").load(fileLocation)
    val columnNames = inputDF.columns
    val columneDataTypes = inputDF.schema.fields.map(x=>x.dataType).map(x=>x.toString)
    val booleanTest = schemaGeneration.checkSchema(inputDF, tableName,columnNames,columneDataTypes,userName, timeStampWrite)
    piiScoring.score(inputDF, booleanTest, tableName, columnNames, userName, timeStampWrite)
    Metadata.generation(inputDF, tableName, columnNames, userName, timeStampWrite)
  }
}

