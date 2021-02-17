package com.pravat.sutar.analytics.spark.json.parsing

/*
* The Spack Code to flatten the nested JSON data and stores the output in 
* CSV format
*
* Author: Pravat SUTAR
*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.udf


object SparkNestedJSONParsing {
	def main (args: Array[String]): Unit = { 
		val spark: SparkSession = SparkSession.builder()
			.master("local[1]")
			.appName("SparkApp to Flatten the Nested JSON")
			.getOrCreate()

	val dataFile="C://hadoop_home/mydata/sample_nested_json_data.json"
  val df:DataFrame=spark.read.option("multiline", "true").json(dataFile)
	df.registerTempTable("df");
	df.printSchema();

	val dataFrame = flattenDataFrame(df)
			dataFrame.show()

			println(s"Write the output in CSV format")
			dataFrame.write
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.save("C://hadoop_home/test/sample_output_data_csv")   
	}
	def flattenDataFrame(dFrame: DataFrame): DataFrame = {
			val fields = dFrame.schema.fields
					val fieldNames = fields.map(x => x.name)

					for (i <- fields.indices) {
						val field = fields(i)
								val fieldType = field.dataType
								val fieldName = field.name
								fieldType match {
								case _: ArrayType =>
								val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
								val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(
										s"explode_outer($fieldName) as $fieldName"
										)
								val explodedDf = dFrame.selectExpr(fieldNamesAndExplode: _*)
								return flattenDataFrame(explodedDf)
								case structType: StructType =>
								val childFieldNames =
								structType.fieldNames.map(childname => fieldName + "." + childname)
								val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
								import org.apache.spark.sql.functions.col

								val renamedCols =
								newFieldNames.map { x =>
								col(x.toString).as(x.toString.replace(".", "_"))
								}

								val explodedDf = dFrame.select(renamedCols: _*)
										return flattenDataFrame(explodedDf)
								case _ =>
						}
					}

			dFrame
	}		
} 
