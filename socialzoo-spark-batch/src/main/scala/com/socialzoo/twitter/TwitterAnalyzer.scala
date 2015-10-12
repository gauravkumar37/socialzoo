package com.socialzoo.twitter

import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TwitterAnalyzer {

	def main(args: Array[String]): Unit = {
		execute(None)
	}

	def execute(master: Option[String]) {
		val sc = {
			val conf = new SparkConf().setAppName("TwitterAnalyzer").setMaster(master.getOrElse("local[*]"))
			new SparkContext(conf)
		}
		val sqlContext = new SQLContext(sc)
		val df = sqlContext.read.avro("socialzoo-spark-batch\\src\\main\\resources\\*.avro")
		// val df = sqlContext.read.avro("/user/gauravk/topics/tweets9/hourly/*/*/*/*")
		println("Total = " + df.count())

		val keywords: Seq[String] = Seq("messi", "chelsea")
		//		val keywords: Seq[String] = Seq("messi", "chelsea", "newcastle", "fifa", "football", "engvswal")
		// (keyword, tweet)
		val rddPairKeywords: RDD[(String, Row)] = df.flatMap(mapTweetsToKeywords(_, keywords))

		// Hourly tweets count aggregations
		// ((keyword, hour), count)
		val rddPairHourlyCounts: RDD[((String, String), Int)] = rddPairKeywords.map {
			case (keyword, row) =>
				((keyword, UtilTwitter.formatDateTime(row.getAs[Long]("created_at"), "yyyy-MM-dd HH")), 1)
		}.reduceByKey(_ + _)
		rddPairHourlyCounts.foreach(println)

		// Daily tweets count aggregations
		// ((keyword, day), count)
		val rddPairDateHourlyCounts: RDD[((String, String), Int)] = rddPairHourlyCounts.map {
			case ((keyword, hour), count) =>
				((keyword, UtilTwitter.reformatDateTime(hour, "yyyy-MM-dd HH", "yyyy-MM-dd")), count)
		}.reduceByKey(_ + _)
		rddPairDateHourlyCounts.foreach(println)

		// ((keyword, hour, word), count)
		val rddPairHourlyWordFreq: RDD[((String, String, String), Int)] = rddPairKeywords.map {
			case (keyword, row) =>
				((keyword, UtilTwitter.formatDateTime(row.getAs[Long]("created_at"), "yyyy-MM-dd HH")), row.getAs[String]("text"))
		}.flatMapValues {
			UtilTwitter.tokenizeText
		}.map {
			case ((keyword, hour), word) =>
				((keyword, hour, word), 1)
		}.reduceByKey(_ + _)
		//		rddPairHourlyWordFreq.foreach(println)

		val rddPairDailyWordFreq: RDD[((String, String, String), Int)] = rddPairHourlyWordFreq.map {
			case ((keyword, hour, word), count) =>
				((keyword, UtilTwitter.reformatDateTime(hour, "yyyy-MM-dd HH", "yyyy-MM-dd"), word), count)
		}.reduceByKey(_ + _)
		rddPairDailyWordFreq.foreach(println)
	}

	def mapTweetsToKeywords(row: Row, keywords: Seq[String]): Seq[(String, Row)] = {
		var ret: ArrayBuffer[(String, Row)] = ArrayBuffer.empty
		for (keyword <- keywords) {
			try {
				if (searchTweet(row, keyword) ||
					searchTweet(row.getAs[Row]("retweeted_status"), keyword) ||
					searchTweet(row.getAs[Row]("quoted_status"), keyword)) {
					ret += ((keyword, row))
				}
			} catch {
				case e: Exception =>
					println("Exception: " + e + ", Tweet ID: " + row.getAs[Long]("id"))
			}
		}
		ret
	}

	def searchTweet(row: Row, keyword: String): Boolean = {
		if (row == null)
			return false
		row.getAs[String]("text").toLowerCase.contains(keyword) ||
			row.getAs[Row]("user").getAs[String]("name").equalsIgnoreCase(keyword) ||
			row.getAs[Row]("user").getAs[String]("screen_name").equalsIgnoreCase(keyword)
	}
}
