package com.socialzoo.twitter

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

import com.databricks.spark.avro._

object TwitterAnalyzer {

	private val AppName = "TwitterAnalyzer"

	def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil) {
		val sc = {
			val conf = new SparkConf().setAppName(AppName).setJars(jars).setMaster(master.getOrElse("local[*]"))
			new SparkContext(conf)
		}
		val sqlContext = new SQLContext(sc)
		val df = sqlContext.read.avro("/user/gauravk/topics/tweets9/hourly/*/*/*/*");
		println(df.count())
	}

	def main(args: Array[String]): Unit = {
		execute(None, Nil, Nil)
	}
}
