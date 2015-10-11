package com.socialzoo.twitter

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

object Util {

	def formatDateTime(epochMilli: Long, pattern: String): String = {
		val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.of("UTC"))
		val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
		dateTime.format(dateTimeFormatter)
	}

	def reformatDateTime(sourceDateTime: String, sourcePattern: String, targetPattern: String) =
		LocalDateTime.parse(sourceDateTime, DateTimeFormatter.ofPattern(sourcePattern)).format(DateTimeFormatter.ofPattern(targetPattern))
}
