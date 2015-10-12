package com.socialzoo.twitter

object UtilTwitter extends Util {

	def tokenizeText(text: String): Array[String] = {
		text.toLowerCase.split(" ").filter(_.length > 2)
	}
}
